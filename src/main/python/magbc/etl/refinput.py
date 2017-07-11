from smv import *
from pyspark.sql.functions import *

class Diags(SmvCsvStringData):
    def schemaStr(self):
        return "diagCode:String;diagType:String"

    def dataStr(self):
        breast = ["174", "174.0" ,"174.1" ,"174.2" ,"174.3" ,"174.4" ,"174.5", "174.6" ,"174.8" ,
            "174.9" ,"175.0" ,"175.9" ,"233.0" ,"239.3"]

        lung_met = ["197.4", "197.1", "197.8", "197.5", "197.7", "197.3", "197.2", "197.0", "197",
            "197.6", "162.8", "162", "162.0", "162.9", "162.2", "162.5", "162.3", "162.4", "239.9",
            "239.89", "239.7", "239.81", "239.2", "239.3", "239.1", "239.8", "239.6", "239.4", "239.5",
            "239", "239.0", "231.0", "231.1", "231.2", "231.8", "231.9", "231"]

        brain_met = ["191.8", "191", "191.2", "191.6", "191.4", "198.3", "191.7", "191.9",
            "191.1", "191.0", "239.6", "191.5", "191.3", "198.81", "198.2", "198",
            "198.4", "198.0", "198.8", "198.5", "198.82", "198.89", "198.6", "198.1", "198.7"]

        liver_met = ["211.9", "211.8", "211", "211.3", "211.5", "211.2", "211.0", "211.4", "211.1", "211.7", "211.6",
            "155", "155.0", "155.2", "155.1", "209.79", "209.74", "209.14", "209.41", "209.31", "209.51",
            "209.70", "209.62", "209.32", "209.52", "209.29", "209.11", "209.63", "209.67", "209.26",
            "209.35", "209.65", "209.66", "209.72", "209.69", "209.56", "209.23", "209.71", "209.22",
            "209.53", "209.24", "209.00", "209.75", "209.43", "209.15", "209.42", "209.01", "209.25",
            "209.30", "209.21", "209.13", "209.40", "209.10", "209.60", "209.73", "209.03", "209.27",
            "209.50", "209.16", "209.17", "209.64", "209.54", "209.61", "209.12", "209.55", "209.34",
            "209.20", "209.57", "209.33", "209.36", "209.02", "197.4", "197.1", "197.8", "197.5", "197.7",
            "197.3", "197.2", "197.0", "197", "197.6", "230.4", "230.6", "230.5", "230", "230.1", "230.3", "230.2",
            "230.8", "230.9", "230.7", "230.0", "235.7", "235.8", "235.5", "235.6", "235.2", "235.4", "235.1", "235.9",
            "235.3", "235.0", "235"]

        bone_met = ["239.9", "239.89", "239.7", "239.81", "239.2", "239.3", "239.1", "239.8", "239.6", "239.4", "239.5",
            "239", "239.0", "170", "170.0", "170.5", "170.3", "170.2", "170.8", "170.7", "170.9", "170.6", "170.1",
            "170.4", "213.9", "213.7", "213.0", "213.2", "213.3", "213.6", "213.1", "213.8", "213.5", "213", "213.4",
            "238.3", "238.5", "238.72", "238", "238.7", "238.75", "238.0", "238.6", "238.71", "238.79", "238.76",
            "238.9", "238.2", "238.73", "238.74", "238.8", "238.1", "238.77", "238.4", "209.79", "209.74",
            "209.14", "209.41", "209.31", "209.51", "209.70", "209.62", "209.32", "209.52", "209.29",
            "209.11", "209.63", "209.67", "209.26", "209.35", "209.65", "209.66", "209.72", "209.69",
            "209.56", "209.23", "209.71", "209.22", "209.53", "209.24", "209.00", "209.75", "209.43",
            "209.15", "209.42", "209.01", "209.25", "209.30", "209.21", "209.13", "209.40", "209.10",
            "209.60", "209.73", "209.03", "209.27", "209.50", "209.16", "209.17", "209.64", "209.54",
            "209.61", "209.12", "209.55", "209.34", "209.20", "209.57", "209.33", "209.36", "209.02"]

        lymph_met = ["196.6", "196.9", "196.2", "196.0", "196.5", "196.1", "196.8", "196.3", "196"]

        crc_met = ["154.2", "154.3", "154.0", "154.1", "154.8", "154", "153.1", "153.0",
            "153.6", "153.5", "153.3", "153.8", "153.2", "153", "153.4", "153.9", "153.7"]

        hn_met = ["171.6", "171.2", "171.8", "171.7", "171.5", "171.3", "171", "171.9", "171.4", "171.0"]

        ovarian_met = ["183", "183.0", "183.5", "183.9", "183.3", "183.4", "183.2", "183.8"]

        panc_met = ["157.4", "157.8", "157.2", "157.1", "157.3", "157.0", "157.9", "157"]

        other_met = ["199.1"]

        return ";\n".join(
                [x + ",breast" for x in breast] +
                [x + ",lung_met" for x in lung_met] +
                [x + ",brain_met" for x in brain_met] +
                [x + ",liver_met" for x in liver_met] +
                [x + ",bone_met" for x in bone_met] +
                [x + ",lymph_met" for x in lymph_met] +
                [x + ",crc_met" for x in crc_met] +
                [x + ",hn_met" for x in hn_met] +
                [x + ",ovarian_met" for x in ovarian_met] +
                [x + ",panc_met" for x in panc_met] +
                [x + ",other_met" for x in other_met]
            )

class Drugs(SmvCsvStringData):
    def schemaStr(self): return "drugName:String;genericName:String;type:String;bcStage:String;jCode:String;form:String"
    def dataStr(self):
        return """Herceptin,Trastuzumab,TARGET,EARLY/METASTATIC,J9355,IV;
                  Perjeta,Pertuzumab,TARGET,EARLY/METASTATIC,J9306,IV;
                  Tykerb,Lapatinib,TARGET,METASTATIC,,ORAL;
                  Kadcyla,ado-trastuzumab,TARGET,METASTATIC,J9354,IV;
                  Adriamycin,Doxorubicin,CHEMO,EARLY/METASTATIC,J9000,IV;
                  Ellence,Epirubicin,CHEMO,EARLY/METASTATIC,J9178,IV;
                  Taxol,Paclitaxel,CHEMO,EARLY/METASTATIC,"J9265,J9267",IV;
                  Taxotere,Docetaxel,CHEMO,EARLY/METASTATIC,"J9170,J9171",IV;
                  Adrucil,Fluorouracil,CHEMO,METASTATIC,J9190,IV;
                  Cytoxan,Cyclophosphamide,CHEMO,EARLY/METASTATIC,"J9070,J8530","IV,ORAL";
                  Paraplatin,Carboplatin,CHEMO,EARLY/METASTATIC,J9045,IV;
                  Platinol,Cisplatin,CHEMO,METASTATIC,J9060,IV;
                  Navelbine,Vinorelbine,CHEMO,METASTATIC,J9390,IV;
                  Xeloda,Capecitabine,CHEMO,METASTATIC,"J8520,J8521",ORAL;
                  DOXIL,DOXORUBICIN HCL PEG-LIPOSOMAL,CHEMO,METASTATIC,"Q2048,Q2049,Q2050",IV;
                  Gemzar,Gemcitabine,CHEMO,METASTATIC,J9201,IV;
                  Ixempra,Ixabepilone,CHEMO,METASTATIC,J9207,IV;
                  Abraxane,Paclitaxel,CHEMO,METASTATIC,J9264,IV;
                  Halaven,eribulin,CHEMO,METASTATIC,J9179,IV;
                  Maxtrex,Methotrexate,CHEMO,EARLY/METASTATIC,"J9250,J9260","IV,IM,ORAL";
                  Faslodex,fulvestrant,HORMONE,METASTATIC,J9395,IM;
                  Arimidex,anastrozole,HORMONE,METASTATIC,,ORAL;
                  SOLTAMOX,TAMOXIFEN,HORMONE,EARLY/METASTATIC,,ORAL;
                  femara,Letrozole,HORMONE,METASTATIC,,ORAL;
                  AROMASIN,EXEMESTANE,HORMONE,EARLY/METASTATIC,,ORAL;
                  Afinitor,Everolimus,HORMONE,METASTATIC,J7527,ORAL;
                  Ibrance,Palbociclib,HORMONE,METASTATIC,,ORAL;"""

    def run(self, df):
        return df.withColumn("drugName", upper(df.drugName)
            ).withColumn("genericName", upper(df.genericName)
            ).withColumn("jCode", split(df.jCode, ","))


class Procs(SmvCsvStringData):
    def schemaStr(self): return "procCode:String;procType:String;subType:String;procDesc:String"
    def dataStr(self):
        return """19081,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. stereotactic guidance. first lesion";
                  19082,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. stereotactic guidance. each additional lesion";
                  19083,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. ultrasound guidance. first lesion";
                  19084,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. ultrasound guidance. each additional lesion";
                  19085,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. magnetic resonance guidance. first lesion";
                  19086,Biopsy,,"Breast biopsy, with placement of localization device and imaging of biopsy specimen, percutaneous. magnetic resonance guidance. each additional lesion";
                  19100,Biopsy,,"Breast biopsy, percutaneous, needle core, not using imaging guidance";
                  19101,Biopsy,,"Breast biopsy, open, incisiona";
                  19102,Biopsy,,"Breast biopsy, percutaneous needle core, using imaging guidance";
                  19103,Biopsy,,"Breast biopsy, percutaneous automated vacuum assisted or rotating biopsy device using imaging guidance (Mammatome)";
                  19120,Biopsy,,"Excision of cyst, fibroadenoma or other benign or malignant tumor, aberrant breast tissue, duct lesion, nipple or areolar lesion. open. one or more lesions";
                  19125,Biopsy,,Excision of breast lesion identified by preoperative placement of radiological marker. open. single lesion;
                  19126,Biopsy,,"Excision of breast lesion identified by preoperative placement of radiological marker, open. each additional lesion separately identified by a preoperative radiological marker";
                  19281,Biopsy,,"Placement of breast localization device, percutaneous. mammographic guidance. first lesion";
                  19282,Biopsy,,"Placement of breast localization device, percutaneous. mammographic guidance. each additional lesion";
                  19283,Biopsy,,"Placement of breast localization device, percutaneous. stereotactic guidance. first lesion";
                  19284,Biopsy,,"Placement of breast localization device, percutaneous. stereotactic guidance. each additional lesion";
                  19285,Biopsy,,"Placement of breast localization device, percutaneous. ultrasound guidance. first lesion";
                  19286,Biopsy,,"Placement of breast localization device, percutaneous. ultrasound guidance. each additional lesion";
                  19287,Biopsy,,"Placement of breast localization device, percutaneous. magnetic resonance guidance. first lesion";
                  10021,Biopsy,,FINE NEEDLE ASPIRATION W/O IMAGING GUIDANCE;
                  10022,Biopsy,,FINE NEEDLE ASPIRATION WITH IMAGING GUIDANCE;
                  20555,Biopsy,,PLACEMENT NEEDLES MUSCLE SUBSEQUENT RADIOELEMENT;
                  76096,Biopsy,,"MAMMOGRAPHIC GUIDANCE, NEEDLE PLACEMENT, BREAST, EACH LESION, RADIOLOGICAL S&I";
                  77011,Biopsy,,CT GUIDANCE STEREOTACTIC LOCALIZATION;
                  77012,Biopsy,,CT GUIDANCE NEEDLE PLACEMENT;
                  77021,Biopsy,,MR GUIDANCE NEEDLE PLACEMENT;
                  77031,Biopsy,,STRTCTC LOCLZJ GID BREAST BX/NEEDLE PLACEMENT;                  
                  88172,Biopsy,,CYTP FINE NDL ASPIRATE IMMT CYTOHIST STD DX 1ST;
                  88173,Biopsy,,CYTP EVAL FINE NEEDLE ASPIRATE INTERP & REPORT;
                  95874,Biopsy,,NEEDLE EMG GUID W/CHEMODENERVATION;
                  76098,Radio,,"Radiological examination, surgical specimen";
                  71010,Radio,,RADIOLOGIC EXAMINATION CHEST SINGLE VIEW FRONTAL;
                  71015,Radio,,RADIOLOGIC EXAMINATION CHEST STERO FRONTAL;
                  71020,Radio,,RADIOLOGIC EXAM CHEST 2 VIEWS FRONTAL&LATERAL;
                  76641,Ultrasound,,"Ultrasound, complete examination of breast including axilla, unilateral";
                  76642,Ultrasound,,"Ultrasound, limited examination of breast including axilla, unilateral";
                  76645,Ultrasound,,"Ultrasound breast(s), Bilateral or Unilateral";
                  76942,Ultrasound,,"Ultrasonic guidance for needle placement, imaging supervision and interpretation";
                  77051,Mammogram,,"Computer-aided detection (computer algorithm analysis of digital image data for lesion detection) with further review for interpretation, with or without digitization of film radiographic images. diagnostic mammography (List separately in addition to code for primary procedure)";
                  77052,Mammogram,,"Computer-aided detection (computer algorithm analysis of digital image data for lesion detection) with further review for interpretation, with or without digitization of film radiographic images. screening mammography (List separately in addition to code for primary procedure)";
                  77053,Mammogram,,"Mammary ductogram or galactogram, single duct";
                  77055,Mammogram,,"Diagnostic Mammogram, Unilateral";
                  77056,Mammogram,,"Diagnostic Mammogram, Bilateral,";
                  77065,Mammogram,,"Diagnostic mammography, unilateral, includes CAD";
                  77066,Mammogram,,"Diagnostic mammography, bilateral, includes CAD";
                  G0204,Mammogram,,"Diagnostic mammogram, digital, bilateral";
                  G0206,Mammogram,,"Diagnostic mammogram, digital, unilateral";
                  G0279,Mammogram,,"Diagnostic digital breast tomosynthesis, unilateral or bilateral";
                  77061,Mammogram,,Digital breast tomosynthesis. unilateral;
                  77062,Mammogram,,Digital breast tomosynthesis. bilateral;
                  77063,Mammogram,,"Screening digital breast tomosynthesis, bilateral";
                  77067,Mammogram,,"Screening mammography, bilateral";
                  G0202,Mammogram,,"Screening mammogram, digital, bilateral";
                  77057,Mammogram,,"Screening mammography, bilateral (2-view film study of each breast)";
                  3014F,Mammogram,,SCREENING MAMMOGRAPHY RESULTS DOC&REV;
                  3340F,Mammogram,,MAMMO ASSESSMENT CAT INCOMP ADDTNL IMAGE DOCD;
                  3341F,Mammogram,,MAMMO ASSESSMENT CAT NEGATIVE DOCD;
                  3342F,Mammogram,,MAMMO ASSESSMENT CAT BENIGN DOCD;
                  3343F,Mammogram,,MAMMO ASSESSMENT CAT PROB BENIGN DOCD;
                  3344F,Mammogram,,MAMMO ASSESSMENT CAT SUSPICIOUS DOCD;
                  3345F,Mammogram,,MAMMO ASSESSMENT CAT HIGH CHANCE MALIG DOCD;
                  3350F,Mammogram,,MAMMO ASSESSMENT CAT BIOPSY PROVEN MALIG DOCD;
                  77032,Mammogram,,MAMMOGRAPHIC GID NEEDLE PLACEMENTT BREAST;
                  81211,Test,BRCA,"BRCA1, BRCA2 (breast cancer 1 and 2) (eg, hereditary breast and ovarian cancer) gene analysis. full sequence analysis and common duplication/deletion variants in BRCA1 (ie, exon 13 del 3.835kb, exon 13 dup 6kb, exon 14-20 del 26kb, exon 22 del 510bp, exon 8-9 del 7.1kb)";
                  81212,Test,BRCA,"BRCA1, BRCA2 (breast cancer 1 and 2) (eg, hereditary breast and ovarian cancer) gene analysis. 185delAG, 5385insC, 6174delT variants";
                  81213,Test,BRCA,"BRCA1, BRCA2 (breast cancer 1 and 2) (eg, hereditary breast and ovarian cancer) gene analysis. uncommon duplication/deletion variants";
                  81214,Test,BRCA,"BRCA1 (breast cancer 1) (eg, hereditary breast and ovarian cancer) gene analysis. full sequence analysis and common duplication/deletion variants (ie, exon 13 del 3.835kb, exon 13 dup 6kb, exon 14-20 del 26kb, exon 22 del 510bp, exon 8-9 del 7.1kb)";
                  81215,Test,BRCA,"BRCA1 (breast cancer 1) (eg, hereditary breast and ovarian cancer) gene analysis. known familial variant";
                  81216,Test,BRCA,"BRCA2 (breast cancer 2) (eg, hereditary breast and ovarian cancer) gene analysis. full sequence analysis";
                  81217,Test,BRCA,"BRCA2 (breast cancer 2) (eg, hereditary breast and ovarian cancer) gene analysis. known familial variant";
                  88341,Test,IHC,"Immunohistochemistry or immunocytochemistry, per specimen. each additional single antibody stain procedure (list separately in addition to code for primary procedure)";
                  88344,Test,IHC,"Immunohistochemistry or immunocytochemistry, per specimen. each multiplex antibody stain procedure";
                  88360,Test,IHC,"Morphometric analysis, tumor immunohistochemistry (e.g. HER-2/neu, estrogen receptor/progesterone receptor), quantitative or semiquantitative, per specimen, each single antibody stain procedure. manual";
                  88361,Test,IHC,using computer-assisted technology;
                  88364,Test,ISH,"In situ hybridization (e.g., FISH), per specimen. each additional single probe stain procedure (list separately in addition to code for primary procedure)";
                  88365,Test,ISH,"In situ hybridization (e.g. FISH), per specimen. initial single probe stain procedure";
                  88367,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative). using computer-assisted technology, per specimen. initial single probe stain procedure";
                  88368,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative), manual, per specimen. initial single probe stain procedure";
                  88369,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative), manual, per specimen. each additional single probe stain procedure (list separately in addition to code for primary procedure)";
                  88373,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative), using computer-assisted technology, per specimen. each additional single probe stain procedure (list separately in addition to code for primary procedure)";
                  88374,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative), using computer-assisted technology, per specimen. each multiplex probe stina procedure";
                  88377,Test,ISH,"Morphometric analysis, in situ hybridization, (quantitative or semi-quantitative), manual, per specimen. each multiplex probe stain procedure"; 
                  77058,MRI,,"Magnetic resonance imaging, breast, without and/or with contrast material(s). unilateral";
                  77059,MRI,,"Magnetic resonance imaging, breast, without and/or with contrast material(s). bilateral";
                  0159T,MRI,,COMPUTER AIDED DETECTION BREAST MRI;
                  C8903,MRI,,"MAGNETIC RESONANCE IMAGING WITH CONTRAST, BREAST";
                  C8906,MRI,,"MAGNETIC RESONANCE IMAGING WITH CONTRAST, BREAST";
                  19110,Surgery,,NIPPLE EXPLORATION;
                  19112,Surgery,,EXCISION LACTIFEROUS DUCT FISTULA;
                  19160,Surgery,Partial,"MASTECTOMY, PARTIAL";
                  19162,Surgery,Partial,"MASTECTOMY, PARTIAL. W/AXILLARY LYMPHADENECTOMY";
                  19297,Surgery,Partial,PLMT EXPANDABLE CATH BRST CONCURRENT PRTL MAST;
                  19301,Surgery,Partial,MASTECTOMY PARTIAL;
                  19302,Surgery,Partial,MASTECTOMY PARTIAL W/AXILLARY LYMPHADENECTOMY;  
                  19303,Surgery,Simple Total,MASTECTOMY SIMPLE COMPLETE;
                  19304,Surgery,Subcutaneous, MASTECTOMY SUBCUTANEOUS;
                  19305,Surgery,Radical,MAST RAD W/PECTORAL MUSCLES AXILLARY LYMPH NODES;
                  19306,Surgery,Radical,MAST RAD W/PECTORAL MUSC AX INT MAM LYMPH NODES;
                  19307,Surgery,Modified Radical,MAST MODF RAD W/AX LYMPH NOD W/WO PECT/ALIS MIN;
                  19240,Surgery,Modified Radical,"MASTECTOMY, MODIFIED RADICAL, W/AXILLARY LYMPH NODES, W/WO PECTORALIS MINOR, W/O PECTORALIS MAJOR" """
                  



