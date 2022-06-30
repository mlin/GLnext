#!/bin/bash
set -o pipefail

if [[ -z $SPARK_HOME ]]; then
    >&2 echo "environment SPARK_HOME required"
    exit 1
fi
if ! [[ -f /tmp/dv1KGP_ALDH2_gvcf.tar ]]; then
    (curl -LSs https://raw.githubusercontent.com/wiki/mlin/spVCF/dv1KGP_ALDH2_gvcf.tar > /tmp/dv1KGP_ALDH2_gvcf.tar) || exit 1
fi

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="$SOURCE_DIR/test/bash-tap"
source "$BASH_TAP_ROOT/bash-tap-bootstrap"

plan tests 5

rm -f target/*.jar
mvn package -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=warn
is "$?" "0" "build"

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/vcfGLuer_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

tar xf /tmp/dv1KGP_ALDH2_gvcf.tar

ls -1 $(pwd)/dv1KGP_ALDH2_gvcf/*vcf.gz > dv1KGP.manifest
MANIFEST=dv1KGP.manifest

if [[ -n $DV1KGP_5PCT ]]; then
    awk 'NR%20==0 { print; }' dv1KGP.manifest > dv1KGP.5pct.manifest
    MANIFEST=dv1KGP.5pct.manifest
fi

export SPARK_LOCAL_IP=127.0.0.1
export LD_LIBRARY_PATH="$SOURCE_DIR/dx/vcfGLuer/resources/usr/lib"
#_JAVA_OPTIONS='-Dconfig.override.joint.gt.overlapMode=REF'

time "${SPARK_HOME}/bin/spark-submit" \
    --master 'local[*]' --driver-memory 8G \
    --name vcfGLuer --class vcfGLuer \
    $SOURCE_DIR/target/vcfGLuer-*.jar $@ --manifest "$MANIFEST" dv1KGP.out

ls -l dv1KGP.out

test -f dv1KGP.out/zzEOF.bgz
is "$?" "0" "vcfGLuer"
cat dv1KGP.out/*.bgz > dv1KGP.vcf.gz
bcftools view dv1KGP.vcf.gz > dv1KGP.vcf
is "$?" "0" "bcftools view"
tabix dv1KGP.vcf.gz
is "$?" "0" "tabix"
test -f dv1KGP.vcf.gz.tbi
is "$?" "0" "tbi"
