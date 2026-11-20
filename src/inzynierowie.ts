import {
  MritDwrClient,
  INZYNIEROWIE_CFG,
  parseDwrTable,
  buildColumnMap,
  sleep,
} from './mrit-dwr.ts';
import { createDbPool, tableName, IS_SHADOW_MODE } from './db.ts';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const PAGE_DELAY_MS = 1000;
const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const TABLE = tableName('inzynierowie');
// Ile czasu wpis może nie pokazywać się w portalu MRiT, zanim uznamy go za wykreślony.
const KARENCJA_USUNIECIA = '3 days';
// Więcej usunięć w jednym przebiegu = raczej awaria portalu niż prawdziwe wykreślenia.
const MAX_USUNIEC_NA_PRZEBIEG = 50;

type StopReason = 'koniec_bazy' | 'awaria_serwera' | 'brak_rekordow';

async function main() {
  console.log(`🤖 START scraper inżynierów | shadow=${IS_SHADOW_MODE} | table=${TABLE}`);

  const client = new MritDwrClient(INZYNIEROWIE_CFG);
  const db = createDbPool();

  try {
    await client.initSession();
    console.log('✅ Sesja zainicjowana');

    let pageNum = 1;
    let lastFirstId = '';
    let lastPageWasFull = true;
    let stopReason: StopReason | null = null;
    let totalUpserted = 0;

    while (stopReason === null) {
      const { rowsProcessed, firstId } = await processPage(client, db, pageNum);

      if (rowsProcessed === null) {
        stopReason = 'awaria_serwera';
        break;
      }

      totalUpserted += rowsProcessed;

      if (firstId !== null && lastFirstId !== '') {
        const curr = parseInt(firstId, 10);
        const last = parseInt(lastFirstId, 10);
        if (curr === last || curr < last - 500) {
          console.log(`🛑 Koniec bazy osiągnięty na stronie ${pageNum}`);
          stopReason = 'koniec_bazy';
          break;
        }
      }

      if (rowsProcessed === 0) {
        stopReason = !lastPageWasFull || pageNum > 500 ? 'koniec_bazy' : 'brak_rekordow';
        console.log(`🛑 Zero rekordów na stronie ${pageNum} → ${stopReason}`);
        break;
      }

      console.log(`✔ Strona ${pageNum} — upsert ${rowsProcessed}`);
      if (firstId) lastFirstId = firstId;
      lastPageWasFull = rowsProcessed >= 100;
      pageNum++;
      await sleep(PAGE_DELAY_MS);
    }

    if (stopReason === 'koniec_bazy') {
      // Kogo nie było w tym przebiegu. Tylko log, bez kasowania: portal MRiT potrafi
      // na chwilę nie pokazać pojedynczego wpisu (19.11 zniknął 41084, choć wpis jest ważny).
      const brakujacy = await db.query<{ inzynier_id: string }>(
        `SELECT inzynier_id FROM ${TABLE}
         WHERE ostatnio_widziany < (NOW() - INTERVAL '2 hours')
         ORDER BY inzynier_id LIMIT 50`,
      );
      if (brakujacy.rowCount) {
        console.log(
          `👀 Nie widać w tym przebiegu (pierwsze 50): ${brakujacy.rows.map((r) => r.inzynier_id).join(', ')}`,
        );
      }

      // Usuwamy dopiero po karencji i tylko gdy MRiT nie przyjął od tej osoby
      // świadectwa wystawionego po dniu, w którym ostatnio była w wykazie.
      const WARUNEK_USUNIECIA = `
        i.ostatnio_widziany < (NOW() - INTERVAL '${KARENCJA_USUNIECIA}')
        AND NOT EXISTS (
          SELECT 1 FROM public.swiadectwa_energetyczne s
          WHERE s.inzynier_id = i.inzynier_id
            AND s.data_wystawienia > (i.ostatnio_widziany AT TIME ZONE 'Europe/Warsaw')::date
        )`;
      const kandydaci = await db.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM ${TABLE} i WHERE ${WARUNEK_USUNIECIA}`,
      );
      const doUsuniecia = kandydaci.rows[0]?.n ?? 0;

      if (doUsuniecia > MAX_USUNIEC_NA_PRZEBIEG) {
        console.log(
          `⛔ ${doUsuniecia} osób do wykreślenia naraz (limit ${MAX_USUNIEC_NA_PRZEBIEG}). Nic nie kasuję, sprawdź portal MRiT.`,
        );
        process.exitCode = 1;
      } else {
        const deleteResult = await db.query(`DELETE FROM ${TABLE} i WHERE ${WARUNEK_USUNIECIA}`);
        console.log(`🗑️ Wykreślono z bazy: ${deleteResult.rowCount} (karencja ${KARENCJA_USUNIECIA})`);
        console.log(`✨ KONIEC — upserted ${totalUpserted}, deleted ${deleteResult.rowCount}`);
      }
    } else {
      console.log(`⚠️ Zakończono bez DELETE (stopReason=${stopReason}, upserted=${totalUpserted})`);
      process.exitCode = 1;
    }
  } finally {
    await db.end();
  }
}

async function processPage(
  client: MritDwrClient,
  db: ReturnType<typeof createDbPool>,
  pageNum: number,
): Promise<{ rowsProcessed: number | null; firstId: string | null }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await client.goToPage(pageNum);
      const { headers, rows } = parseDwrTable(response);

      if (rows.length === 0 && !response.includes('<table')) {
        throw new Error('Brak tabeli w odpowiedzi');
      }

      const colMap = buildColumnMap(headers, {
        id: (h) => h.includes('numer wpisu'),
        imie: (h) => h.includes('imię') || h.includes('imiona'),
        nazwisko: (h) => h.includes('nazwisko'),
        data: (h) => h.includes('data wpisu'),
      });

      let firstId: string | null = null;
      const toUpsert: Array<[string, string, string, string | null]> = [];

      for (const [idx, row] of rows.entries()) {
        if (row.length < 4) continue;

        const inzynierId = row[colMap.id!];
        const imie = row[colMap.imie!];
        const nazwisko = row[colMap.nazwisko!];
        const dataWpisu = row[colMap.data!];

        if (idx === 0) firstId = inzynierId;
        if (!/^\d+$/.test(inzynierId) || !nazwisko) continue;

        toUpsert.push([
          inzynierId,
          imie,
          nazwisko,
          DATE_REGEX.test(dataWpisu) ? dataWpisu : null,
        ]);
      }

      if (toUpsert.length > 0) {
        const values: string[] = [];
        const params: (string | null)[] = [];
        toUpsert.forEach((tuple, i) => {
          const base = i * 4;
          values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, NOW())`);
          params.push(...tuple);
        });
        await db.query(
          `INSERT INTO ${TABLE} (inzynier_id, "imię", nazwisko, data_wpisu, ostatnio_widziany)
           VALUES ${values.join(', ')}
           ON CONFLICT (inzynier_id)
           DO UPDATE SET "imię" = EXCLUDED."imię",
                         nazwisko = EXCLUDED.nazwisko,
                         data_wpisu = EXCLUDED.data_wpisu,
                         ostatnio_widziany = NOW()`,
          params,
        );
      }

      return { rowsProcessed: toUpsert.length, firstId };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`⚠️ Błąd na str. ${pageNum} (próba ${attempt}/${MAX_RETRIES}): ${msg}`);
      if (attempt === MAX_RETRIES) {
        console.log(`❌ CRASH na stronie ${pageNum}`);
        return { rowsProcessed: null, firstId: null };
      }
      await sleep(RETRY_DELAY_MS);
    }
  }
  return { rowsProcessed: null, firstId: null };
}

/**
 * Przejściowe błędy sieci (DNS/timeout) — MRiT chwilowo padło.
 * Exit 0 → workflow OK, brak spamu maili przy każdym tickz pg_cron.
 */
function isTransientNetworkError(err: unknown): boolean {
  const e = err as { cause?: { code?: string; hostname?: string }; code?: string; message?: string };
  const code = e?.cause?.code || e?.code;
  const TRANSIENT_CODES = ['EAI_AGAIN', 'ENOTFOUND', 'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'EPIPE', 'UND_ERR_SOCKET'];
  if (code && TRANSIENT_CODES.includes(code)) return true;
  if (typeof e?.message === 'string' && /fetch failed/i.test(e.message)) return true;
  return false;
}

main().catch((err) => {
  if (isTransientNetworkError(err)) {
    const e = err as { cause?: { code?: string; hostname?: string }; code?: string };
    const code = e?.cause?.code || e?.code || 'UNKNOWN';
    const host = e?.cause?.hostname || 'mrit.gov.pl';
    console.warn(`⏸️ Przejściowy błąd sieci (${code} @ ${host}) — pomijam ten tick, retry za 60s.`);
    process.exit(0);
  }
  console.error('💥 Fatal:', err);
  process.exit(1);
});
