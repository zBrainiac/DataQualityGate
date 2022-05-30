# example data quality gate
### build project
```
git clone https://github.com/zBrainiac/DataQualityGate.git
cd DataQualityGate
sbt assembly
```

### check output folder:
```
ls -l target/scala-2.12/
```

### submit classes local-mode
```
spark-submit --class abc_SchemaValidator_4711 target/scala-2.12/DataQualityGate_0.1.0.jar -Pinput_filename=amazon_reviews_us_Jewelry_v1_00_short.tsv input_path=./data/input/ output_path=./data/output/
spark-submit --class abc_DataValidator_4711 target/scala-2.12/DataQualityGate_0.1.0.jar -Pinput_filename=amazon_reviews_us_Jewelry_v1_00_short.tsv input_path=./data/input/ output_path=./data/output/
spark-submit --class abc_DataProfilerEnhanced_4711 target/scala-2.12/DataQualityGate_0.1.0.jar -Pinput_filename=amazon_reviews_us_Jewelry_v1_00_short.tsv input_path=./data/input/ output_path=./data/output/
```

### run classes in standalone/local cluster
```
cd /opt/homebrew/Cellar/apache-spark/3.2.1/libexec/sbin
./start-all.sh

# submit:
./bin/spark-submit --class abc_SchemaValidator_4711 --master spark://mdaeppen-MBP14:7077 --deploy-mode client /Users/marceldaeppen/GoogleDrive/workspace/DataQualityGate/target/scala-2.12/DataQualityGate_0.1.0.jar -Pinput_filename=amazon_reviews_us_Jewelry_v1_00_short.tsv input_path=./data/input/ output_path=./data/output/
```
