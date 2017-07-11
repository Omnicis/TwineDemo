# LungCancer

After moving to the NeoDM structure, this project should be significantly simplified.
Current project code is from Phase 1 code, without cleaning up. Phase 2 work should be mainly under
com.omnicis.mag.lung2.



Project has library level dependency to
* https://github.com/TresAmigosSD/SMV
* https://github.com/Omnicis/TwineCommonData
* https://github.com/Omnicis/NeoDM

So when you update those 3 project from github (`git pull`), you need to re-install (`mvn install`) them
in the order of SMV, and then TwineCommonData and NeoDM.

##[Set up data dir](docs/data_dir_structure.md)

##[Project Package Structure](docs/packageStructure.md)

##[SMV User Guide](https://github.com/TresAmigosSD/SMV/blob/master/docs/user/0_user_toc.md)

##[Config project to use TwineCommonData published output](https://github.com/Omnicis/TwineCommonData/blob/master/README.md)
