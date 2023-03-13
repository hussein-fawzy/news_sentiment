news_sentiment is a project that can be used to analyze the stock market sentiment for a given symbol.

Two types of sentiments are obtained:

1- Read News -> Analyse Sentiment -> Store Results
<br>
Sentiment is analyzed using VADER (Valence Aware Dictionary and sEntiment Reasoner)
    
2- Read Social Sentiment -> Store Results

News and social Sentiemnt are obtained from Financial Modeling Prep (https://site.financialmodelingprep.com/)

Usage example:

```
#make sure to add the Financial Modeling Prep API Key to config.py

from fmp import FMP

symbol = "MSFT"
symbol_fmp = FMP(symbol)

symbol_fmp.read_news(verbose = True)
symbol_fmp.add_sentiment_to_news()
#a new storage file should contain the news and their sentiment

symbol_fmp.read_social_sentiment(verbose = True)
#another storage file is created with the social sentiment
```
