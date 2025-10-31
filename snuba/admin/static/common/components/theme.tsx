import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";

export const theme = EditorView.theme({
  "&": {
    fontSize: "14px",
  },
});

export const highlighting = syntaxHighlighting(
  HighlightStyle.define([
    {
      tag: [tags.standard(tags.tagName), tags.tagName],
      color: "#116329",
      fontSize: 10,
    },
    { tag: [tags.comment, tags.bracket], color: "#6a737d" },
    { tag: [tags.className, tags.propertyName], color: "#6f42c1" },
    {
      tag: [tags.variableName, tags.attributeName, tags.number, tags.operator],
      color: "#005cc5",
    },
    {
      tag: [tags.keyword, tags.typeName, tags.typeOperator, tags.typeName],
      color: "#d73a49",
    },
    { tag: [tags.string, tags.meta, tags.regexp], color: "#032f62" },
    { tag: [tags.name, tags.quote], color: "#22863a" },
    { tag: [tags.heading, tags.strong], color: "#24292e", fontWeight: "bold" },
    { tag: [tags.emphasis], color: "#24292e", fontStyle: "italic" },
    { tag: [tags.deleted], color: "#b31d28", backgroundColor: "ffeef0" },
    {
      tag: [tags.atom, tags.bool, tags.special(tags.variableName)],
      color: "#e36209",
    },
    { tag: [tags.url, tags.escape, tags.regexp, tags.link], color: "#032f62" },
    { tag: tags.link, textDecoration: "underline" },
    { tag: tags.strikethrough, textDecoration: "line-through" },
    { tag: tags.invalid, color: "#cb2431" },
  ]),
);
