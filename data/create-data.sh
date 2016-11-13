rm -fr raw zip gzip
mkdir raw zip gzip

pip install faker dicttoxml

python create-data.py

cd gzip
cp ../raw/* .
gzip -9 *

cd ../raw
zip ../zip/logfiles1.zip logfile_[0-2]
zip ../zip/logfiles2.zip logfile_[3-5]
zip ../zip/logfiles3.zip logfile_[6-8]
cd ..