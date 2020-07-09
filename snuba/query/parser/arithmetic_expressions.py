from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


grammar = Grammar(
    r"""
    expression = (term operator expression) / term
    operator = "*" / "+" / "/" / "-"
    term = ~"[1-9]\d*"

"""
)


class PrefixVisitor(NodeVisitor):
    def visit_term(self, node, children):
        return node.text

    def visit_operator(self, node, children):
        return node.text

    def visit_expression(self, node, children):
        child = children[0]
        # print(child)

        if len(child) == 1:
            return [child]

        else:
            if (child[1] == "*" or child[1] == "/") and len(child[2]) > 1:
                if child[2][1] == "*" or child[2][1] == "/":
                    child[2].insert(2, child[0])
                    child[2].insert(2, child[1])

                else:
                    child[2].insert(1, child[0])
                    child[2].insert(1, child[1])

            else:
                child[2].insert(0, child[0])
                child[2].insert(0, child[1])
        return child[2]

    def generic_visit(self, node, children):
        return children or node.text


exp_tree = grammar.parse("8/5*4+3-2")
parsed_exp = PrefixVisitor().visit(exp_tree)
print(parsed_exp)
