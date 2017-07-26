# VOC Demo

This project is an example variation of care analysis for demo purpose.

Project has library level dependency to SMV branch 2.1, which in trun depends on Spark 2.1.1
* https://github.com/TresAmigosSD/SMV/tree/2.1

Project's data input is located at:
* Twine01:/data/pdda_raw/twine_demo

Project's library uses:
* ClaimUtils: 
  * used in magbc.claim.etl.PrimPhyn 
  * source code in twinelib.utils.ClaimUtils
  * specification inside module
* HierarchyUtils: 
  * used in magbc.geo.geoagg.AllStatsGeoLevel
  * source code in twinelib.hier.HierarchyUtils
  * specification inside module
* MatcherUtils: 
  * used in magbc.matcher.physicianmatch.PhysicianMatch
  * source code in twinelib.matcher.MatcherUtils
  * specification inside module
* LoTRE: 
  * used in magbc.claim.lot.Lot
  * source code in twinelib.relib.RE.lotre._
  * specification defined (outside module) in twinelib.relib.rules._
