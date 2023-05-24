from pathlib import Path
import sys

DATA_DIR = Path(r"../data")
LSW_DIR = DATA_DIR / "lhm4.3/coupling"
LKM25_DIR = DATA_DIR / "lkm25/Schematisatie/KRWVerkenner/shapes"
LHM_DIR = DATA_DIR / "lhm4.3"
MOZART_DIR = LHM_DIR / "mozart"
LSM_KOPPELING_DIR = DATA_DIR / "koppeling_lsm3_lhm3.4"

def load_src():
    src_path = Path(r"..\src").absolute().resolve()
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))