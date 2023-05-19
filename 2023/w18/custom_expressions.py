import polars as pl

@pl.api.register_expr_namespace('custom')
class CustomStringMethodsCollection:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def to_title_case(self) -> pl.Expr:
        '''
        '''
        convert_to_title = (
            pl.element().str.slice(0, 1).str.to_uppercase() 
            + 
            pl.element().str.slice(1).str.to_lowercase()
            )
        
        converted_elements = (
            self._expr
            .str.split(' ')
            .arr.eval(convert_to_title)
            .arr.join(separator=' ')
            )
        return converted_elements
