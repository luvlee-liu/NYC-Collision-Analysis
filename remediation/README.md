## This script will create alert if nearby location have more incidents in NYC

run `./queryNearbyDanger.py <current latitude> <current longitude>`

examples:
```
cd ./remediation
$ ./queryNearbyDanger.py  40.7339903 -73.9867068
current at: https://www.google.com/maps/search/?api=1&query=40.7339903,-73.9867068
SAFE: keep eye on the road

$ ./queryNearbyDanger.py  40.7334832 -73.9867383
current at: https://www.google.com/maps/search/?api=1&query=40.7334832,-73.9867383
DANGER: slow down and be cautious at:
{
    "name": "EAST 14 STREET,3 AVENUE",
    "distance": 2.8911783228359393e-15,
    "at": "https://www.google.com/maps/search/?api=1&query=40.7334832,-73.9867383",
    "number_incident": 281,
    "casualty_per_incident": 0.24199288256227758,
    "severity": "DANGER"
}
```
