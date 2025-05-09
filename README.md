# CN230 Project Template

    python db.py


1. fork this repository
2. run codespaces
3. when done execute the following git command

```
    git add .
    git commit -m "finished"
    git push origin main
```

My Project Description:
nasdaq100.csv = name (symbol) from nasdaq100 index
stock.py = the algorithm that would use the symbol stock from nasdaq100.csv to load stock detail (30 days) into database
nasdaq100_price.db = the database that store the detail (symbol, date, open, high, low, close, volume)
analyze.py = the algorithm that would analyze the nasdaq100_price.db