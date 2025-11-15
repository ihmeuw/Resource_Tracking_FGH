
# Arg 1: year to pass to rake.py
# Arg 2: tag to pass to rake.py

PYTHON=FILEPATH
SCRIPT=FILEPATH/rake.py

$PYTHON -u $SCRIPT --year $1 --tag $2 --work_dir $3