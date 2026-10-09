import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";
import { SOLARIZED } from "SnubaAdmin/theme";

export const theme = EditorView.theme(
  {
    "&": {
      fontSize: "14px",
      color: SOLARIZED.BASE1,
      backgroundColor: SOLARIZED.BASE03,
      border: `1px solid ${SOLARIZED.BASE01}`,
      borderRadius: "4px",
    },
    ".cm-content": { caretColor: SOLARIZED.BASE1 },
    ".cm-cursor, .cm-dropCursor": { borderLeftColor: SOLARIZED.BASE1 },
    "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection":
      { backgroundColor: SOLARIZED.BASE02 },
    ".cm-activeLine": { backgroundColor: "transparent" },
    ".cm-gutters": {
      backgroundColor: SOLARIZED.BASE02,
      color: SOLARIZED.BASE01,
      border: "none",
    },
  },
  { dark: true },
);

export const highlighting = syntaxHighlighting(
  HighlightStyle.define([
    {
      tag: [tags.standard(tags.tagName), tags.tagName],
      color: SOLARIZED.GREEN,
      fontSize: 10,
    },
    { tag: [tags.comment, tags.bracket], color: SOLARIZED.BASE01 },
    { tag: [tags.className, tags.propertyName], color: SOLARIZED.VIOLET },
    {
      tag: [tags.variableName, tags.attributeName, tags.number, tags.operator],
      color: SOLARIZED.BLUE,
    },
    {
      tag: [tags.keyword, tags.typeName, tags.typeOperator, tags.typeName],
      color: SOLARIZED.GREEN,
    },
    { tag: [tags.string, tags.meta, tags.regexp], color: SOLARIZED.CYAN },
    { tag: [tags.name, tags.quote], color: SOLARIZED.YELLOW },
    { tag: [tags.heading, tags.strong], color: SOLARIZED.BASE2, fontWeight: "bold" },
    { tag: [tags.emphasis], color: SOLARIZED.BASE2, fontStyle: "italic" },
    { tag: [tags.deleted], color: SOLARIZED.RED },
    {
      tag: [tags.atom, tags.bool, tags.special(tags.variableName)],
      color: SOLARIZED.ORANGE,
    },
    { tag: [tags.url, tags.escape, tags.regexp, tags.link], color: SOLARIZED.CYAN },
    { tag: tags.link, textDecoration: "underline" },
    { tag: tags.strikethrough, textDecoration: "line-through" },
    { tag: tags.invalid, color: SOLARIZED.RED },
  ]),
);
