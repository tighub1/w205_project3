Start redis graph with
```
docker run -p 6379:6379 -it --rm redislabs/redisgraph
```
To initialize the redis database with train station data
```
python redis_store.py
```
To run the redis script to simulate traffic changes every 10 minutes:
```
crontab -e
```

and then enter this in the file
```
*/10 * * * * python redis_store.py
```