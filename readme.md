# Energy Consumption Based Business Operating Hours Estimator

## Description

A solution to estimate the operating hours of our customers at each site (NMI) by assessing variation in their consumption level across time


## Requirements

You'll need installed the following packages:
* [pytz](https://pypi.org/project/pytz/)
* [pandas](https://pandas.pydata.org/)


## Input files

* consumption data files in csv format - mandatory columns are AESTTime, Quantity, Unit
* nmi_info.csv - contains essential information on NMIs in consumptions data files

Only NMIs that exist in both nmi_info.csv and consumption data file will be processed by the estimator


## Executing program

```
$ python main.py {nmi info file} {consumption data folder} {processed data folder}
eg. python main.py nmi_info.csv ConsumptionData processed_data
```


## Authors

Eric Lee  


## Version History

* 0.1
    * Initial Release


## License

[MIT](https://choosealicense.com/licenses/mit/)