import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
import glow
import sys

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('-g', '--gene_list_file', required=True,
                    help='A text file that contains the list of gene. Each row in the text file should correspond to one gene. No header required.')
# parser.add_argument('-s', '--study_ids', nargs='+', default=[], required=True,
#                     help='list of study to look for')
parser.add_argument('--hgmd_var',
        help='HGMD variant parquet file dir')
parser.add_argument('--dbnsfp',
        help='dbnsfp annovar parquet file dir')
parser.add_argument('--clinvar', action='store_true', help='Include ClinVar data')
parser.add_argument('--consequences', action='store_true', help='Include consequences data')
parser.add_argument('--variants', action='store_true', help='Include variants data')
parser.add_argument('--diagnoses', action='store_true', help='Include diagnoses data')
parser.add_argument('--phenotypes', action='store_true', help='Include phenotypes data')
parser.add_argument('--occurrences',
        help='occurrences parquet file dir')
parser.add_argument('--maf', default=0.0001,
        help='gnomAD and TOPMed max allele frequency')
parser.add_argument('--dpc_l', default=0.5,
        help='damage predict count lower threshold')
parser.add_argument('--dpc_u', default=1,
        help='damage predict count upper threshold')
parser.add_argument('--known_variants_l', nargs='+', default=['ClinVar', 'HGMD'],
                    help='known variant databases used, default is ClinVar and HGMD')
parser.add_argument('--aaf', default=0.2,
        help='alternative allele fraction threshold')
parser.add_argument('--output_basename', default='gene-based-variant-filtering',
        help='Recommand use the task ID in the url above as output file prefix. \
        For example 598b5c92-cb1d-49b2-8030-e1aa3e9b9fde is the task ID from \
    https://cavatica.sbgenomics.com/u/yiran/variant-workbench-testing/tasks/598b5c92-cb1d-49b2-8030-e1aa3e9b9fde/#set-input-data')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of Spark executor instances')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of Spark executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1, help='Spark driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300, help='Spark SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Number of Spark driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Spark driver memory in GB')

args = parser.parse_args()

# Create spark session
args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
gene_text_path = args.gene_list_file
# study_id_list = args.study_ids
gnomAD_TOPMed_maf = args.maf
dpc_l = args.dpc_l
dpc_u = args.dpc_u
known_variants_l = args.known_variants_l
aaf = args.aaf
output_basename = args.output_basename
occurrences_path = args.occurrences

# get a list of interested gene and remove unwanted strings in the end of each gene
gene_symbols_trunc = spark.read.option("header", False).text(gene_text_path)
gene_symbols_trunc = list(gene_symbols_trunc.toPandas()['value'])
gene_symbols_trunc = [gl.replace('\xa0', '').replace('\n', '') for gl in gene_symbols_trunc]

# customized tables loading
hg38_HGMD_variant = spark.read.parquet(args.hgmd_var)
dbnsfp_annovar = spark.read.parquet(args.dbnsfp)
if args.clinvar:
    clinvar = spark.read.format("delta") \
        .load("s3a://kf-strides-public-vwb-prd/clinvar")
if args.consequences:
    consequences = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/consequences')
if args.variants:
    variants = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/variants')
if args.diagnoses:
    diagnoses = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/disease')
if args.phenotypes:
    phenotypes = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/normalized/phenotype')
occurrences = spark.read.parquet(occurrences_path)

# provided study id by occurences table
# Find the start and end index of the desired substring
start_index = occurrences_path.index("occurrences_sd") + len("occurrences_sd")
end_index = occurrences_path.index("_re_", start_index)
# Extract the desired substring
substring = occurrences_path[start_index-2:end_index]
# Convert the substring to uppercase
study_id_list = [substring.upper()]

# read multi studies
# occ_dict = []
# for s_id in study_id_list:
#     occ_dict.append(spark.read.parquet(occurrences_parent_path + '/occurrences_*/study_id=' + s_id))
# occurrences = reduce(DataFrame.unionAll, occ_dict)

# gene based variant filtering
def gene_based_filt(gene_symbols_trunc, study_id_list, gnomAD_TOPMed_maf, dpc_l, dpc_u,
                known_variants_l, aaf, hg38_HGMD_variant, dbnsfp_annovar, clinvar, 
                consequences, variants, diagnoses, phenotypes, occurrences):
    #  Actual running step, generating table t_output
    cond = ['chromosome', 'start', 'reference', 'alternate']

    # Table consequences, restricted to canonical annotation and input genes/study IDs
    c_csq = ['consequence', 'vep_impact', 'symbol', 'ensembl_gene_id', 'refseq_mrna_id', 'hgvsc',
            'hgvsp']
    t_csq = consequences.where((F.col('original_canonical') == 'true') \
                & (F.col('symbol').isin(gene_symbols_trunc))) \
        .select(cond + c_csq)
    chr_list = [c['chromosome'] for c in t_csq.select('chromosome').distinct().collect()]

    # Table dbnsfp_annovar, added a column for ratio of damage predictions to all predictions
    c_dbn = ['DamagePredCount', 'PredCountRatio_D2T', 'TWINSUK_AF', 'ALSPAC_AF', 'UK10K_AF']
    t_dbn = dbnsfp_annovar \
        .where(F.col('chromosome').isin(chr_list)) \
        .withColumn('PredCountRatio_D2T',
                    F.when(F.split(F.col('DamagePredCount'), '_')[1] == 0, F.lit(None).cast(DoubleType())) \
                    .otherwise(
                        F.split(F.col('DamagePredCount'), '_')[0] / F.split(F.col('DamagePredCount'), '_')[1])) \
        .select(cond + c_dbn)

    # Table variants, added a column for max minor allele frequency among gnomAD and TOPMed databases
    c_vrt_unnested = ['max_gnomad_topmed']
    c_vrt_nested = ["topmed_bravo", 'gnomad_genomes_2_1_1', 'gnomad_exomes_2_1_1', 'gnomad_genomes_3']
    # c_vrt = ['max_gnomad_topmed', 'topmed', 'gnomad_genomes_2_1', 'gnomad_exomes_2_1', 'gnomad_genomes_3_0', 'gnomad_genomes_3_1_1']
    t_vrt = variants \
        .withColumn('max_gnomad_topmed',
                    F.greatest(F.lit(0), F.col('external_frequencies.topmed_bravo')['af'], F.col('external_frequencies.gnomad_genomes_2_1_1')['af'], \
                                F.col('external_frequencies.gnomad_exomes_2_1_1')['af'], F.col('external_frequencies.gnomad_genomes_3')['af'])) \
        .where((F.size(F.array_intersect(F.col('studies.study_id'), F.lit(F.array(*map(F.lit, study_id_list))))) > 0) & \
                F.col('chromosome').isin(chr_list)) \
        .select(*[F.expr(f"external_frequencies.{nc}.af").alias(f"{nc}_af") for nc in c_vrt_nested] + cond + c_vrt_unnested)

    # Table ClinVar, restricted to those seen in variants and labeled as pathogenic/likely_pathogenic
    c_clv = ['VariationID', 'clin_sig', 'conditions']
    t_clv = clinvar \
        .withColumnRenamed('name', 'VariationID') \
        .where(F.split(F.split(F.col('geneinfo'), '\\|')[0], ':')[0].isin(gene_symbols_trunc) \
                & (F.array_contains(F.col('clin_sig'), 'Pathogenic') \
                | F.array_contains(F.col('clin_sig'), 'Likely_pathogenic'))) \
        .join(t_vrt, cond) \
        .select(cond + c_clv)

    # Table ClinVar, restricted to those seen in variants and labeled as pathogenic/likely_pathogenic/vus
    # c_clv = ['VariationID', 'clin_sig']
    # t_clv = spark \
    #     .table('clinvar') \
    #     .withColumnRenamed('name', 'VariationID') \
    #     .where(F.split(F.split(F.col('geneinfo'), '\\|')[0], ':')[0].isin(gene_symbols_trunc) \
    #         & (F.array_contains(F.col('clin_sig'), 'Pathogenic') \
    #         | F.array_contains(F.col('clin_sig'), 'Likely_pathogenic') \
    #         | F.array_contains(F.col('clin_sig'), 'Uncertain_significance'))) \
    #     .join(t_vrt, cond) \
    #     .select(cond + c_clv)

    # Table HGMD, restricted to those seen in variants and labeled as DM or DM?
    c_hgmd = ['HGMDID', 'variant_class', 'phen']
    t_hgmd = hg38_HGMD_variant \
        .withColumnRenamed('id', 'HGMDID') \
        .where(F.col('symbol').isin(gene_symbols_trunc) \
                & F.col('variant_class').startswith('DM')) \
        .join(t_vrt, cond) \
        .select(cond + c_hgmd)

    # Join consequences, variants and dbnsfp, restricted to those with MAF less than a threshold and PredCountRatio_D2T within a range
    t_csq_vrt = t_csq \
        .join(t_vrt, cond)

    t_csq_vrt_dbn = t_csq_vrt \
        .join(t_dbn, cond, how='left') \
        .withColumn('flag', F.when( \
        (F.col('max_gnomad_topmed') < gnomAD_TOPMed_maf) \
        & (F.col('PredCountRatio_D2T').isNull() \
            | (F.col('PredCountRatio_D2T').isNotNull() \
            & (F.col('PredCountRatio_D2T') >= dpc_l) \
            & (F.col('PredCountRatio_D2T') <= dpc_u)) \
            ), 1) \
                    .otherwise(0))

    # Include ClinVar if specified
    if 'ClinVar' in known_variants_l and t_clv.count() > 0:
        t_csq_vrt_dbn = t_csq_vrt_dbn \
            .join(t_clv, cond, how='left') \
            .withColumn('flag', F.when(F.col('VariationID').isNotNull(), 1).otherwise(t_csq_vrt_dbn.flag))

    # Include HGMD if specified
    if 'HGMD' in known_variants_l and t_hgmd.count() > 0:
        t_csq_vrt_dbn = t_csq_vrt_dbn \
            .join(t_hgmd, cond, how='left') \
            .withColumn('flag', F.when(F.col('HGMDID').isNotNull(), 1).otherwise(t_csq_vrt_dbn.flag))

    # Table occurrences, restricted to input genes, chromosomes of those genes, input study IDs, and occurrences where alternate allele
    # is present, plus adjusted calls based on alternative allele fraction in the total sequencing depth
    c_ocr = ['ad', 'dp', 'variant_allele_fraction', 'calls', 'adjusted_calls', 'filter', 'is_lo_conf_denovo', 'is_hi_conf_denovo',
        'is_proband', 'affected_status', 'gender',
        'biospecimen_id', 'participant_id', 'mother_id', 'father_id', 'family_id']
    t_ocr = occurrences.withColumn('variant_allele_fraction', F.col('ad')[1] / (F.col('ad')[0] + F.col('ad')[1])) \
        .withColumn('adjusted_calls', F.when(F.col('variant_allele_fraction') < aaf, F.array(F.lit(0), F.lit(0)))
                                    .otherwise(F.col('calls'))) \
        .where(F.col('chromosome').isin(chr_list) \
                & (F.col('is_multi_allelic') == 'false') \
                & (F.col('has_alt') == 1) \
                & (F.col('adjusted_calls') != F.array(F.lit(0), F.lit(0)))) \
        .select(cond + c_ocr)

    # Finally join all together
    # t_output = F.broadcast(t_csq_vrt_dbn) \
    t_output = t_csq_vrt_dbn \
        .join(t_ocr, cond) \
        .where(t_csq_vrt_dbn.flag == 1) \
        .drop('flag')

    t_dgn = diagnoses \
        .withColumn('participant_id', F.regexp_replace(F.upper(F.col('participant_fhir_id')), "-", "_")) \
        .select('study_id','participant_id', 'source_text') \
        .distinct() \
        .groupBy('participant_id') \
        .agg(F.collect_list('source_text').alias('diagnoses_combined'), \
            F.first('study_id').alias('study_id')) 
    #### processing phenotypes table
    exploded_pht = phenotypes.select("phenotype_id", F.explode(F.col("condition_coding")).alias("coding"))
    hpo_pht = exploded_pht.filter(F.col("coding.category") == "HPO") \
                        .select("phenotype_id", "coding.code")
    phenotypes = phenotypes.join(hpo_pht, on="phenotype_id", how="left")
    t_pht = phenotypes \
        .withColumn('participant_id', F.regexp_replace(F.upper(F.col('participant_fhir_id')), "-", "_")) \
        .select('participant_id', 'source_text', 'code') \
        .distinct() \
        .groupBy('participant_id') \
        .agg(F.collect_list('source_text').alias('phenotypes_combined'), \
            F.collect_list('code').alias('hpos_combined'))
    
    t_output_dgn_pht = t_output \
        .join(t_dgn, 'participant_id', 'left') \
        .join(t_pht, 'participant_id', 'left')

    return (t_output_dgn_pht)


# define output name and write table t_output
def write_output(t_output, output_basename, study_id_list):
    Date = list(spark.sql("select current_date()") \
                .withColumn("current_date()", F.col("current_date()").cast("string")) \
                .toPandas()['current_date()'])
    output_filename= "_".join(Date) + "_" + output_basename + "_".join(study_id_list) + ".tsv.gz"
    t_output.toPandas() \
        .to_csv(output_filename, sep="\t", index=False, na_rep='-', compression='gzip')

if args.hgmd_var is None:
    print("Missing hgmd_var parquet file", file=sys.stderr)
    exit(1)
if args.dbnsfp is None:
    print("Missing dbnsfp parquet file", file=sys.stderr)
    exit(1)
if args.clinvar is None:
    print("Missing clinvar parquet file", file=sys.stderr)
    exit(1)
if args.consequences is None:
    print("Missing consequences parquet file", file=sys.stderr)
    exit(1)
if args.variants is None:
    print("Missing variants parquet file", file=sys.stderr)
    exit(1)
if args.diagnoses is None:
    print("Missing diagnoses parquet file", file=sys.stderr)
    exit(1)
if args.phenotypes is None:
    print("Missing phenotypes parquet file", file=sys.stderr)
    exit(1)

t_output = gene_based_filt(gene_symbols_trunc, study_id_list, gnomAD_TOPMed_maf, dpc_l, dpc_u,
                    known_variants_l, aaf, hg38_HGMD_variant, dbnsfp_annovar, clinvar, 
                    consequences, variants, diagnoses, phenotypes, occurrences)
write_output(t_output, output_basename, study_id_list)