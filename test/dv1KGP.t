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

BASH_TAP_ROOT="test/bash-tap"
source test/bash-tap/bash-tap-bootstrap

plan tests 2

rm -f target/*.jar
mvn package
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
export SPARK_LOCAL_IP=127.0.0.1
export LD_LIBRARY_PATH="$SOURCE_DIR/dx/vcfGLuer/resources/usr/lib"
#_JAVA_OPTIONS='-Dconfig.override.joint.gt.overlapMode=REF'
time "${SPARK_HOME}/bin/spark-submit" \
    --master 'local[*]' --driver-memory 8G \
    --name vcfGLuer --class vcfGLuer \
    $SOURCE_DIR/target/vcfGLuer-*.jar $@ --manifest dv1KGP.manifest dv1KGP_out

test -f dv1KGP_out/_EOF.bgz
is "$?" "0" "vcfGLuer run"
