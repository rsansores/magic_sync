// The engine's times, read exactly, and the server's clock as the page sees
// it: every time in an answer is the server's, so how long ago one was is
// measured against the server's `now`, never against the browser's clock.

const RFC3339 =
  /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:([Zz])|([+-])(\d{2}):(\d{2}))$/

/** An RFC 3339 time as milliseconds since the epoch, whatever the digits of
 * its fraction; `null` for anything else. */
export function parseTimestamp(text: string): number | null {
  const m = RFC3339.exec(text)
  if (!m) return null
  const [y, mo, d, h, mi, s] = m.slice(1, 7).map(Number) as [number, number, number, number, number, number]
  const [fraction = '', , sign, oh, om] = m.slice(7)
  const wall = Date.UTC(y, mo - 1, d, h, mi, s, Number(fraction.slice(0, 3).padEnd(3, '0')))
  const read = new Date(wall)
  // Date.UTC rolls a 13th month or a 31st of April over; a time does not.
  if (read.getUTCMonth() !== mo - 1 || read.getUTCDate() !== d || h > 23 || mi > 59 || s > 59) return null
  const offset = sign ? (sign === '-' ? -1 : 1) * (Number(oh) * 60 + Number(om)) * 60_000 : 0
  return wall - offset
}

/** `parseTimestamp`, carrying a missing time through. */
export function timeOf(text: string | null): number | null {
  return text == null ? null : parseTimestamp(text)
}

/** The server's time now: its `now` when it answered, moved on by the time
 * that has passed in the page since the answer arrived. `null` when the
 * answer's time does not read. */
export function serverNow(answered: string, receivedAt: number, current: number): number | null {
  const at = parseTimestamp(answered)
  return at == null ? null : at + (current - receivedAt)
}
