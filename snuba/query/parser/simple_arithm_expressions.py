from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

from snuba.query.expressions import FunctionCall, Literal

grammar = Grammar(
    r"""
    expression = term (("+" expression) / space)
    term = factor (("*" term) / space)
    factor = ~"[1-9]\d*"
    space = ''

"""
)

# TODO: Expand grammar to include "-" and "/" operators.


class PrefixVisitor(NodeVisitor):
    def visit_space(self, node, children):
        return

    def visit_factor(self, node, children):
        return Literal(None, int(node.text))

    def visit_term(self, node, children):
        factor, term = children

        if all(x is None for x in term):
            # A check for any None child nodes
            # that arose from empty space ''
            return factor
        else:
            return FunctionCall(None, term[0][0], (factor, term[0][1]))

    def visit_expression(self, node, children):
        term, exp = children

        if all(x is None for x in exp):
            return term
        else:
            return FunctionCall(None, exp[0][0], (term, exp[0][1]))

    def generic_visit(self, node, children):
        return children or node.text


exp_tree = grammar.parse("5*4*3+2*1+6")
parsed_exp = PrefixVisitor().visit(exp_tree)
print(parsed_exp)
