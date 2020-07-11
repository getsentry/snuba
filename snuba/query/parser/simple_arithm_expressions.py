from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

from snuba.query.expressions import Literal
from snuba.query.dsl import plus, multiply


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
        print(children)
        factor, term = children

        if term is None:
            # A check for any None child nodes
            # that arose from empty space ''
            return factor
        else:
            # return FunctionCall(None, term[0][0], (factor, term[0][1]))
            return multiply(factor, term[1])

    def visit_expression(self, node, children):
        print(children)
        term, exp = children

        if exp is None:
            return term
        else:
            # return FunctionCall(None, exp[0][0], (term, exp[0][1]))
            return plus(term, exp[1])

    def generic_visit(self, node, children):
        if isinstance(children, list) and len(children) == 1:
            return children[0]

        return children or node.text


exp_tree = grammar.parse("7*5*4*3+2*1+6")
parsed_exp = PrefixVisitor().visit(exp_tree)
print(parsed_exp)
