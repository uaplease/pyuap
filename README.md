# pyuap
Python Utilities and Analysis Package for Unidentified Anomalous Phenomena 

## data

todos:
- waterufo
- uaplease
- nuforc
- mufon

## usage

```python
from pyuap import data
reports = data.WaterUFONet(buffer_time=10).get_case_reports()
```