# Project structure

For phase1 and phase2, we had Mag project depend on both pdda part and gene part of the NeoDM porject. However the gene part of NeoDM
was Lung only, so need to anyhow build the gene tsdm for Breast Cancer. Also after the gSonarLung project, we are getting familiar
with the gSonar layer 2 tables, we should build our tsdm on top of that.

Then the question is should we develop gene-tsdm within Mag project or within NeoDM. To keep a relatively simple approach, let's start
here in Mag. Also since we do not need a full tsdm for magbc, let's do the minimal purely for this project.

# Stages
`magbc` should have 3 stages:
* `tsdm`
* `bcvc` - Breast Cancer Variation of Care
* `ui`

# `tsdm` Design

## Output
Ptnt claims with
* Primary Phys
* HCOS map to org
* Cohort label
* Main drugs (chemo, hormone, targeted)
* Main tests
* Duration
* Test to drug
* Operation to drug
* Meta to drug
