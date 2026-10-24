import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

// Every text the default look draws, against the surface it is drawn on,
// must reach WCAG's 4.5:1.

const css = readFileSync(new URL('./dashboard.css', import.meta.url), 'utf8')

/** The default of each `--_name: var(--msd-name, #rrggbb)` token. */
const defaults = Object.fromEntries(
  [...css.matchAll(/--_([\w-]+):\s*var\(--msd-[\w-]+,\s*(#[0-9a-f]{6})\)/gi)].map((m) => [m[1]!, m[2]!]),
)

function luminance(hex: string): number {
  const [r, g, b] = [1, 3, 5].map((i) => {
    const c = parseInt(hex.slice(i, i + 2), 16) / 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r! + 0.7152 * g! + 0.0722 * b!
}

function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (hi! + 0.05) / (lo! + 0.05)
}

// [text, surface]: the card under every list and chip, each tint under the
// title and the lines drawn on it, and each tone on its own tint.
const pairs: [string, string][] = [
  ...['fg', 'muted', 'accent', 'ok', 'warn', 'danger', 'quiet', 'off'].map((text): [string, string] => [
    text,
    'card',
  ]),
  ...['ok-soft', 'warn-soft', 'danger-soft', 'quiet-soft', 'off-soft', 'accent-soft'].flatMap(
    (surface): [string, string][] => [
      ['fg', surface],
      ['muted', surface],
    ],
  ),
  // Each tone's badge, chip and counter: its colour on its own tint.
  ['ok', 'ok-soft'],
  ['warn', 'warn-soft'],
  ['danger', 'danger-soft'],
  ['quiet', 'quiet-soft'],
  ['off', 'off-soft'],
  ['accent', 'accent-soft'],
]

describe('the default colours', () => {
  it('are all read from the stylesheet', () => {
    for (const [text, surface] of pairs)
      expect([text, surface].map((k) => defaults[k])).not.toContain(undefined)
  })

  it.each(pairs)('draw %s on %s at 4.5:1 or more', (text, surface) => {
    expect(contrast(defaults[text]!, defaults[surface]!)).toBeGreaterThanOrEqual(4.5)
  })
})
