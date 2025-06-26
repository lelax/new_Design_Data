import pandas as pd

class TriplestoreQueryProcessor:
    def __init__(self, endpoint):
        self.endpoint = endpoint  # SPARQL endpoint

    def searchByAuthor(self, name: str) -> pd.DataFrame:
        query = f"""
        SELECT DISTINCT ?publication ?title ?date
        WHERE {{
            ?publication a :Publication ;
                         :hasTitle ?title ;
                         :hasDate ?date ;
                         :hasAuthor ?author .
            ?author :hasGivenName ?givenName ;
                    :hasFamilyName ?familyName .
            FILTER(CONTAINS(LCASE(STR(?givenName)), "{name.lower()}") || 
                   CONTAINS(LCASE(STR(?familyName)), "{name.lower()}"))
        }}
        """
        # Assume self.endpoint.query returns a SPARQL result set
        results = self.endpoint.query(query)
        return pd.DataFrame([dict(result) for result in results])
