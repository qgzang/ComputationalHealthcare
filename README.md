# Computational Healthcare Library 
This repository contains Computational Healthcare library (chlib), the underlying library used in [Computational Healthcare](http://www.computationalhealthcare.com/). 
Computational Healthcare library is designed to allow computer scientists to use large healthcare claims databases. Using chlib you can easily load & process large healthcare databases with millions of patients. 

## Analyze up to 200 Million visits & 70 Million patients

Currently we support following three databases:

 1. [Texas Inpatient public use data file 2006-2009](https://www.dshs.texas.gov/thcic/hospitals/Inpatientpudf.shtm): This database contains approximately 11 Million de-identified inpatient visits from Texas during 2006-2009, this data is available as free download. Inpatients visits in this database lack patient identifier.
   
 2. [HCUP Nationwide Readmission database for 2013](https://www.hcup-us.ahrq.gov/nrdoverview.jsp): This database contains de-identified inpatients visits during 2013 & 14. Unlike Texas database all inpatient visits are associated with a patient identifier and its possible to track patient through multiple visits.
 
 3. [HCUP State Inpatient, ED & SASD database](http://www.hcup-us.ahrq.gov/sidoverview.jsp): This is one of the largest logitudinal database of medical claims in the world. Acquiring this database typically takes several weeks and can cost few 100$ to ~10,000$ depending number of states/years/types. We currently support data from California, Florida & New York. If you are interested in using Computational Healthcare with this dataset please contact us.
          
**Please note that this repository does not contains any data, nor do we provide any data. You should acquire the datasets on your own 
  from AHRQ and/or other state agencies.**         

## Architecture & Data Model
A quick overview of data model, architecture is available in this [presentation](https://docs.google.com/presentation/d/1Oh_-FShr3BCGiCSqghI2dQYnyKvVOeiOqkIjaaEOPwc/edit?usp=sharing).

- The library uses Protocol buffers for storing [raw data (visits & patients)](/chlib/entity/protocols/pvisit.proto) and [aggregate statistics](/chlib/entity/protocols/pstat.proto).
- Categorical fields-values are represented as [enums](/chlib/entity/protocols/penums.proto) using Protocol buffers. 
- Raw data is stored in a levelDB database
- Protocol Buffers and LevelDB makes it easy to use any programming language
- We provide code to compute aggregate statistics in privacy preserving manner
- Integrated with TensorFlow for building machine learning models
 
## Installation & Setup

- The [docker folder](docker/) contains a Dockerfile with all dependencies specified. 
It also contains script for building docker image, starting container, preparing databases from user supplied files.

- Once you have obtained data you should modify [docker/prepare_nrd.sh](docker/prepare_nrd.sh) script with correct path to .CSV file and run the script.

- For Texas dataset modify/run the [prepare_tx.sh](docker/prepare_tx.sh).

- The dockerfile uses TensorFlow version 0.11 docker image as a starting point. Thus in addition to Computational Healthcare library it 
also contains a Jupyter notebook server which runs automatically when the container is started. Once the preparation step is 
complete you can use the jupyter notebook server running (inside the container) on port 8888 of your local machine.


## Quick overview  

```python
    import chlib
    NRD = chlib.data.Data.get_from_config('../config.json','HCUPNRD')
    # patients
    for p_key,patient in NRD.iter_patients():
        break
    print p_key,patient
    
    # visits
    for p_key,patient in NRD.iter_patients():
        for v in patient.visits:
            break
        break
    print v
```

#### output (fake)

````
123213213 patient_key: "213213213"
visits {
  key: "123213"
  patient_key: "213123213213"
  dataset: "NRD_2011"
  state: "NRD"
  facility: "1232131"
  vtype: IP
  age: 23
  sex: FEMALE
  race: R_UNKNOWN
  source: S_ED
  disposition: D_ROUTINE
  los: 0
  death: ALIVE
  payer: PRIVATE
  primary_diagnosis: "D80"
  primary_procedure {
    pcode: "P86"
    pday: 0
    ctype: ICD
  }
  drg: "DG05"
  prs {
    pcode: "P8659"
    pday: -1
    ctype: ICD
    occur: 1
  }
  year: 2013
  month: 10
  quarter: 1
  zip: Z_THIRD
  dnr: DNR_UNAVAILABLE
  charge: 229.0
}
raw: "<>"
linked: true

  key: "123213"
  patient_key: "213213213123"
  dataset: "NRD_2011"
  state: "NRD"
  facility: "1232131"
  vtype: IP
  age: 65
  sex: FEMALE
  race: R_UNKNOWN
  source: S_ED
  disposition: D_ROUTINE
  los: 0
  death: ALIVE
  payer: PRIVATE
  primary_diagnosis: "D80"
  primary_procedure {
    pcode: "P86"
    pday: 0
    ctype: ICD
  }
  drg: "DG05"
  dxs: "D709"
  prs {
    pcode: "P8659"
    pday: -1
    ctype: ICD
    occur: 1
  }
  year: 2013
  day: 5151
  month: 10
  quarter: 1
  zip: Z_THIRD
  dnr: DNR_UNAVAILABLE
  charge: 229.0
````
      
### Text description of codes and enums      
```python
    coder =  chlib.codes.Coder() 
    print 'D486',coder['D486'] # ICD-9 diagnosis codes are prepended with 'D'
    print 'P9971',coder['P9971'] # ICD-9 procedure codes are prepended with 'P'
    print coder[chlib.entity.enums.D_AMA] # You can also print string representation of Enums            
```
#### output
````
D486 Pneumonia, organism unspecified
P9971 Therapeutic plasmapheresis
Against medical advice
````

### Retrieve list of patients with particular diagnosis or procedure

```python 
    patients_undergoing_plasmapheresis = [p for _,p in NRD.iter_patients_by_code('P9971')]
    # You can speed this up by precomputing list of patients for each codes, using 'fab precompute'
    print len(patients_undergoing_plasmapheresis)
    for v in patients_undergoing_plasmapheresis[0].visits:
        print v.key,v.day,v.prs
```   

## Tutorials / Articles

1. [Computational Healthcare for reproducible machine learning: building embedding from million inpatient visits](blog/introduction.ipynb)
 
2. [Analyzing long term outcomes of Ventriculostomy in pediatric patients](blog/ventriculostomy.ipynb)

3. [Exploring OHDSI common data model, comparison with Computational Healthcare (currently writing)](blog/ohdsi.ipynb)

## Issues & Bugs
To minimize chances of visit/patient level information leaking via Exceptions messages or Traceback, we have not enabled
issues on github repo. If you find any bugs, make sure that your bug report/question does not contains any visit or patient
 level information. To file a bug please email us at address provided below.

## Contact
For more information, comments or if you plan on citing Computational Healthcare library please contact Akshay Bhat at aub3@cornell.edu.
 
## Copyright
Copyright Cornell University 2016; All rights reserved;
Please contact us for more information.
