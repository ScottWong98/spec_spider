# SPEC Spider

A Spider for SPEC

## Requirements

- Python 3.8
- Scrapy 2.6.1

## Usage

In root folder, use these commands to start spider.
```
$ bash bin/cral_cpu2017.sh
$ bash bin/cral_cpu2006.sh
$ bash bin/cral_jbb2015.sh
$ bash bin/cral_jvm2008.sh
$ bash bin/cral_ssj2008.sh
```

## Data

- CPU
    - CPU2017
        - Floating Point Rates
        - Floating Point Speed
        - Integer Rates
        - Integer Speed
    - CPU2006
        - CINT
        - CFP
        - CINT Rates
        - CFP Rates
- JAVA
    - Jbb2015
    - Jvm2008
- Power
    - ssj2008

The downloaded data is stored in `data/` folder, like this:

```
data
├── cpu2006
│   ├── 2022_05_10_21_05_27
│   │   ├── SPECfp_rate.csv
│   │   ├── SPECint.csv
│   │   ├── SPECint_rate.csv
│   │   └── SPEFfp.csv
├── cpu2017
│   └── 2022_05_11_20_44_51
│       ├── CFP2017_rate.csv
│       ├── CFP2017_speed.csv
│       ├── CINT2017_rate.csv
│       └── CINT2017_speed.csv
├── jbb2015
│   └── 2022_05_11_22_56_03
│       └── jbb2015.csv
├── jvm2008
│   ├── 2022_05_12_11_35_01
│   │   └── jvm2008.csv
```
