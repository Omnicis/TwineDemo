# Fuzzy matching of id's

We implemented four different matcher functions to be used depending on the availability of input:

* fullLevelMatch
  * all levels of input info present (zip, city, hrrnum, hsanum, name, address)
  * match hospitals within shared hrr regions
  * exact match on address_id, then used normlevenshtein to compare normalized values
* noZipMatch
  * use name, address, city and state
  * GroupCondition on state
  * Fuzzy matching on address and name
* noZipNoAddressMatch
  * use name, city, and state
  * GroupCondition on state
  * Fuzzy matching on name using levenshtein and nGram2
* nameAndStateMatch
  * use name and state
  * GroupCondition on state
  * Fuzzy matching on name using levenshtein

for all of these, first normalized name and address:

* all caps
* standardized high frequency words
* removed 'stopwords'

fuzzy match implemented between 2579 hcos and following id's:

* ccn
* pac
* OncCareModel id's
* nccn id's
* nci

## HCOS to CCN

### input
* 4463 ccn id's

### matcher
* fullLevelMatch
* dedup `Org_ID`s on `L5_HRR_Name_Value` and `L2_Address_Value`

### results:

* 557 matched
* 436 distinct CCN
* 56 matched of top 100 accounts (distinct)

## HCOS to PAC

### input
* 6637 pac id's

### matcher
* nameAndStateMatch
* dedup `Org_ID`s on `L1_Name_Value`

### results

* 5237 matched
* 2560 distinct pac id
* 60 matched of top 100 accounts (52 distinct)


## HCOS to COC

### input
* 1296 CoC ids

### matcher
* fullLevelMatch
* dedup `Org_ID`s on `L5_HRR_Name_Value` and `L2_Address_Value`

### results

* 65 matched
* 65 distinct coc id
* 12 matched of top 100

## HCOS to OncCareModel id's

### input
* 195 id's

### matcher
* noZipMatch
* dedup `Org_ID`s on `L3_State_Name_Value` and `L1_AddressOnly_Value`

### results
* 6 matched
* 6 distinct id's
* 19 matched of top 100

## HCOS to NCCN

### input
* 33 nccn id's

### matcher
* noZipNoAddressMatch
* dedup `Org_ID`s on `L1_City_Name_L_Value` and `L3_City_Name_N2_Value`

### results
* 6 matched
* 6 distinct id's
* 7 matched of top 100

## HCOS to Ncid

### input
* 69 id's

### matcher
* fullLevelMatch
* dedup `Org_ID`s on `L5_HRR_Name_Value` and `L2_Address_Value`

### results
* 9 matching
* 9 distinct id's
* 0 matched of top 100
