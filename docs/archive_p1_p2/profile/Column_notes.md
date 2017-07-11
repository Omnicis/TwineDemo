# Treatment measures

## Stage at Diagnosis
* Derived from SHA data
* Diagnosed as advanced stage of a patient is defined as "any of metastatic diagnostic or treatment
  records happened within 60 days after 1st cancer diagnostic"
* The Geo level measure is the percentage of lung cancer patients who "Diagnosed as advanced stage"

## EGFR Test Rate
* By definition EGFR test is the number of patients who have EGFR tests divided by metastatic
  patients.
* Since SHA has systematic coverage bias on the test count, we have to scale the test count on
  each Geo level then divid by the metastatic patient count
* The scale factor is determined by (number of patients who have both EGFR and TARCEVA) vs (number of
  patients who have TARCEVA). The assumption is that whoever been prescribed TARCEVA should have
  EGFR test
