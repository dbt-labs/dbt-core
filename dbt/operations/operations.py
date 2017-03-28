
import jinja2.nodes
import jinja2.ext


class OperationExtension(jinja2.ext.Extension):

    def extract_method_name(self, tokens):
        name = ""
        import ipdb; ipdb.set_trace()
        for token in tokens:
            if token.test("variable_begin"):
                continue
            elif token.test("name"):
                name += token.value
            elif token.test("dot"):
                name += token.value
            else:
                break
        if not name:
            name = "bind#0"
        return name

    def filter_stream(self, stream):
        """
        We convert 
        {{ some.variable | filter1 | filter 2}}
            to 
        {{ some.variable | filter1 | filter 2 | bind}}
        
        ... for all variable declarations in the template
        This function is called by jinja2 immediately 
        after the lexing stage, but before the parser is called. 
        """
        while not stream.eos:
            token = next(stream)

            held = []
            held.append(token)

            if token.test('name') and token.value in ('ref', 'var'):
                next_token = next(stream)
                held.append(next_token)
                if next_token.test('lparen'):
                    raise RuntimeError('Used {} in a macro!'.format(token.value))
            for token in held:
                yield token


def if_already_exists(model, does_exist=True):
    import ipdb; ipdb.set_trace()
    return ""
