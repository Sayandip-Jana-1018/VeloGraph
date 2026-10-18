// Building blocks for the report: typography, figures and tables with numbered captions, data tables, lists.
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

import {
  AlignmentType,
  BorderStyle,
  ExternalHyperlink,
  HeadingLevel,
  ImageRun,
  LevelFormat,
  Paragraph,
  ShadingType,
  SequentialIdentifier,
  Table,
  TableCell,
  TableRow,
  TextRun,
  VerticalAlign,
  WidthType,
} from "docx";

export const COLORS = { ink: "1F2340", teal: "0F766E", sub: "4A5070", rule: "C9CEDD", head: "E8EEF6", soft: "F5F7FB" };
export const FONT = "Calibri";
/** Text width of an A4 page with 2.54 cm margins, in DXA (twentieths of a point) and in pixels at 96 dpi. */
export const TEXT_DXA = 9026;
export const TEXT_PX = 602;

export const styles = {
  default: { document: { run: { font: FONT, size: 22, color: "222222" }, paragraph: { spacing: { line: 288, after: 120 } } } },
  paragraphStyles: [
    {
      id: "Heading1",
      name: "Heading 1",
      basedOn: "Normal",
      next: "Normal",
      quickFormat: true,
      run: { font: FONT, size: 34, bold: true, color: COLORS.ink },
      paragraph: { spacing: { before: 120, after: 240 }, outlineLevel: 0, keepNext: true },
    },
    {
      id: "Heading2",
      name: "Heading 2",
      basedOn: "Normal",
      next: "Normal",
      quickFormat: true,
      run: { font: FONT, size: 27, bold: true, color: COLORS.teal },
      paragraph: { spacing: { before: 300, after: 120 }, outlineLevel: 1, keepNext: true },
    },
    {
      id: "Heading3",
      name: "Heading 3",
      basedOn: "Normal",
      next: "Normal",
      quickFormat: true,
      run: { font: FONT, size: 23, bold: true, color: COLORS.ink },
      paragraph: { spacing: { before: 220, after: 80 }, outlineLevel: 2, keepNext: true },
    },
    {
      // Front matter (declaration, contents, ...): looks like Heading 1 but stays out of the contents.
      id: "FrontHeading",
      name: "Front Heading",
      basedOn: "Normal",
      next: "Normal",
      quickFormat: true,
      run: { font: FONT, size: 34, bold: true, color: COLORS.ink },
      paragraph: { spacing: { before: 120, after: 240 }, keepNext: true },
    },
    {
      id: "Caption",
      name: "Caption",
      basedOn: "Normal",
      next: "Normal",
      run: { font: FONT, size: 18, italics: true, color: COLORS.sub },
      paragraph: { spacing: { before: 80, after: 240 }, alignment: AlignmentType.CENTER },
    },
  ],
};

export const numbering = {
  config: [
    {
      reference: "bullets",
      levels: [
        { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } },
        { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1080, hanging: 270 } } } },
      ],
    },
    {
      reference: "steps",
      levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 360 } } } }],
    },
  ],
};

/** Inline markup: **bold**, `code` and plain text, so paragraphs can be written as strings. */
export function runs(text, base = {}) {
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0;
  for (const m of text.matchAll(re)) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else out.push(new TextRun({ text: t.slice(1, -1), font: "Consolas", size: 19, color: "3B3F5C", ...base }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...base }));
  return out;
}

export const p = (text, opts = {}) =>
  new Paragraph({ children: runs(text, opts.run), alignment: opts.align ?? AlignmentType.JUSTIFIED, spacing: opts.spacing, keepNext: opts.keepNext });
export const h1 = (text) => new Paragraph({ text, heading: HeadingLevel.HEADING_1, pageBreakBefore: true });
export const h1Plain = (text) => new Paragraph({ text, heading: HeadingLevel.HEADING_1 });
/** A front-matter title: a new page, Heading 1's look, not in the contents. */
export const front = (text, first = false) => new Paragraph({ text, style: "FrontHeading", pageBreakBefore: !first });
export const h2 = (text) => new Paragraph({ text, heading: HeadingLevel.HEADING_2 });
export const h3 = (text) => new Paragraph({ text, heading: HeadingLevel.HEADING_3 });
export const bullet = (text, level = 0) =>
  new Paragraph({ children: runs(text), numbering: { reference: "bullets", level }, spacing: { after: 60 } });
export const bullets = (items) => items.map((i) => (Array.isArray(i) ? bullet(i[0], 1) : bullet(i)));
export const steps = (items, instance) =>
  items.map((t) => new Paragraph({ children: runs(t), numbering: { reference: "steps", level: 0, instance }, spacing: { after: 60 } }));
export const link = (text, url) => new ExternalHyperlink({ link: url, children: [new TextRun({ text, style: "Hyperlink" })] });

/** Width and height of a PNG or JPEG, read from its header. */
export function imageSize(path) {
  const b = readFileSync(path);
  if (b[0] === 0x89 && b[1] === 0x50) return { width: b.readUInt32BE(16), height: b.readUInt32BE(20), type: "png" };
  let i = 2;
  while (i < b.length) {
    if (b[i] !== 0xff) break;
    const marker = b[i + 1];
    const len = b.readUInt16BE(i + 2);
    if (marker >= 0xc0 && marker <= 0xc3) return { height: b.readUInt16BE(i + 5), width: b.readUInt16BE(i + 7), type: "jpg" };
    i += 2 + len;
  }
  throw new Error(`cannot read the size of ${path}`);
}

let figureNo = 0;
let tableNo = 0;
const marks = new Map();

/**
 * Figure widths in px by figure number, written by layout.py after it measured the last PDF: a figure that would
 * leave a gap at the bottom of a page is narrowed until it fits there.
 */
const LAYOUT_FILE = join(import.meta.dirname, "layout.json");
const layout = existsSync(LAYOUT_FILE) ? JSON.parse(readFileSync(LAYOUT_FILE, "utf8")) : {};
const widthOf = (n, fallback) => layout[String(n)] ?? fallback;
/**
 * The number the next figure (or table) will get, for a reference in the text just before it; `key` keeps the
 * number for references further on (`refOf`). Captions are numbered in order, so text and caption always agree.
 */
export function mark(key, kind) {
  const n = kind === "table" ? tableNo + 1 : figureNo + 1;
  marks.set(key, n);
  return n;
}
export const refOf = (key) => {
  if (!marks.has(key)) throw new Error(`no figure or table marked ${key}`);
  return marks.get(key);
};

/** An image with no caption or number, for the cover. */
export function plainImage(path, widthPx) {
  const { width, height, type } = imageSize(path);
  return new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 60, after: 240 },
    children: [new ImageRun({ type, data: readFileSync(path), transformation: { width: widthPx, height: Math.round((height / width) * widthPx) } })],
  });
}

/** An image scaled to `widthPx` (at most the text width), with a numbered caption under it. */
export function figure(path, caption, widthPx = TEXT_PX) {
  const { width, height, type } = imageSize(path);
  figureNo += 1;
  const w = Math.min(widthOf(figureNo, widthPx), TEXT_PX);
  return [
    new Paragraph({
      alignment: AlignmentType.CENTER,
      keepNext: true,
      spacing: { before: 120, after: 0 },
      children: [new ImageRun({ type, data: readFileSync(path), transformation: { width: w, height: Math.round((height / width) * w) } })],
    }),
    new Paragraph({
      style: "Caption",
      children: [new TextRun("Figure "), new SequentialIdentifier("Figure"), new TextRun(`: ${caption}`)],
    }),
  ];
}

/** Two images side by side, each with its own caption line, under one figure number. */
export function figurePair([a, b], caption) {
  const cellW = Math.floor(TEXT_DXA / 2);
  figureNo += 1;
  const w = Math.min(widthOf(figureNo, 292), 292);
  // keepNext on everything in the row keeps the images on the same page as the figure's caption.
  const img = (path) => {
    const { width, height, type } = imageSize(path);
    return new Paragraph({
      alignment: AlignmentType.CENTER,
      keepNext: true,
      children: [new ImageRun({ type, data: readFileSync(path), transformation: { width: w, height: Math.round((height / width) * w) } })],
    });
  };
  const cell = (child, note) =>
    new TableCell({
      width: { size: cellW, type: WidthType.DXA },
      borders: noBorders,
      margins: { top: 40, bottom: 40, left: 60, right: 60 },
      children: [child, new Paragraph({ style: "Caption", keepNext: true, spacing: { before: 40, after: 40 }, children: [new TextRun(note)] })],
    });
  return [
    new Table({
      width: { size: TEXT_DXA, type: WidthType.DXA },
      columnWidths: [cellW, TEXT_DXA - cellW],
      rows: [new TableRow({ children: [cell(img(a.path), a.note), cell(img(b.path), b.note)] })],
    }),
    new Paragraph({ style: "Caption", children: [new TextRun("Figure "), new SequentialIdentifier("Figure"), new TextRun(`: ${caption}`)] }),
  ];
}

const thin = { style: BorderStyle.SINGLE, size: 4, color: COLORS.rule };
export const noBorders = {
  top: { style: BorderStyle.NONE, size: 0, color: "FFFFFF" },
  bottom: { style: BorderStyle.NONE, size: 0, color: "FFFFFF" },
  left: { style: BorderStyle.NONE, size: 0, color: "FFFFFF" },
  right: { style: BorderStyle.NONE, size: 0, color: "FFFFFF" },
};

/**
 * A data table with a shaded header row and a numbered caption above it. `widths` are relative; `align` per
 * column ("left" or "right"; numbers read better right-aligned).
 */
export function table(caption, header, rows, { widths, align = [], fontSize = 19 } = {}) {
  const n = header.length;
  const rel = widths ?? Array(n).fill(1);
  const total = rel.reduce((a, b) => a + b, 0);
  const cols = rel.map((r) => Math.floor((r / total) * TEXT_DXA));
  cols[n - 1] = TEXT_DXA - cols.slice(0, -1).reduce((a, b) => a + b, 0);
  // A table may break across pages (its header row repeats) rather than leave an empty gap behind it; only the
  // header keeps with the first row.
  const cell = (text, i, head) =>
    new TableCell({
      width: { size: cols[i], type: WidthType.DXA },
      borders: { top: thin, bottom: thin, left: thin, right: thin },
      shading: head ? { type: ShadingType.CLEAR, fill: COLORS.head, color: "auto" } : undefined,
      margins: { top: 50, bottom: 50, left: 90, right: 90 },
      verticalAlign: VerticalAlign.CENTER,
      children: [
        new Paragraph({
          alignment: align[i] === "right" ? AlignmentType.RIGHT : AlignmentType.LEFT,
          keepNext: head,
          spacing: { after: 0, line: 252 },
          children: runs(String(text), { size: fontSize, bold: head || undefined, color: head ? COLORS.ink : undefined }),
        }),
      ],
    });
  tableNo += 1;
  return [
    new Paragraph({
      style: "Caption",
      keepNext: true,
      spacing: { before: 160, after: 80 },
      children: [new TextRun("Table "), new SequentialIdentifier("Table"), new TextRun(`: ${caption}`)],
    }),
    new Table({
      width: { size: TEXT_DXA, type: WidthType.DXA },
      columnWidths: cols,
      rows: [
        new TableRow({ tableHeader: true, children: header.map((h, i) => cell(h, i, true)) }),
        ...rows.map((r) => new TableRow({ cantSplit: true, children: r.map((c, i) => cell(c, i, false)) })),
      ],
    }),
    new Paragraph({ spacing: { after: 120 }, children: [] }),
  ];
}

/** A highlighted box for a key takeaway. */
export function callout(title, text) {
  return new Table({
    width: { size: TEXT_DXA, type: WidthType.DXA },
    columnWidths: [TEXT_DXA],
    rows: [
      new TableRow({
        children: [
          new TableCell({
            width: { size: TEXT_DXA, type: WidthType.DXA },
            shading: { type: ShadingType.CLEAR, fill: COLORS.soft, color: "auto" },
            borders: { top: noBorders.top, bottom: noBorders.bottom, right: noBorders.right, left: { style: BorderStyle.SINGLE, size: 24, color: COLORS.teal } },
            margins: { top: 120, bottom: 120, left: 200, right: 200 },
            children: [
              new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: title, bold: true, color: COLORS.teal })] }),
              new Paragraph({ spacing: { after: 0 }, children: runs(text) }),
            ],
          }),
        ],
      }),
    ],
  });
}

export const gap = (after = 120) => new Paragraph({ spacing: { after }, children: [] });
