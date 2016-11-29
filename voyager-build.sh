#!/bin/bash
set -e

usage() {
    echo "$0  [-d] [-r <mvn repsository>] [-o <output path>] <version>"
}

mvn_repo="~/.m2/repository"
output_dir="`pwd`/build"
deploy="no"

OPTIND=1
while getopts "h?r:o:d" opt; do
    case "$opt" in
    h|\?)
        usage
        exit 0
        ;;
    d)
        deploy="yes"
        ;;
    r)
        mvn_repo=$OPTARG
        ;;
    o)
        output_dir=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))
version=$1

if [ -z $version ]; then
  usage
  exit 1
fi

echo "version = $version"
echo "maven repository = $mvn_repo"
echo "output directory = $output_dir"
echo "deploy = $deploy"

ant clean-maven-build

mvn_opts="-Dmaven.repo.local=${mvn_repo}"

# Replace the revision in:
# dev-tools/maven/pom.xml.template:          <revisionOnScmFailure>NO-REVISION-AVAILABLE</revisionOnScmFailure>

REV="$(git describe --always)"
echo "${REV}"
git checkout dev-tools/maven/pom.xml.template

# hack for osx, check for "gsed" (ie gnu sed installed via homebrew)
SED=sed
set +e
gsed --version
if [ "$?" == "0" ]; then
  SED=gsed
fi
set -e

$SED -i 's#NO-REVISION-AVAILABLE#'"${REV}"'#' dev-tools/maven/pom.xml.template 
# rm dev-tools/maven/pom.xml.template.bak

rm -rf ${mvn_repo}/org/apache/solr
rm -rf ${mvn_repo}/org/apache/lucene

ant ivy-bootstrap

# now get the poms with out special version
ant -Dversion=${version} get-maven-poms

# git checkout dev-tools/maven/pom.xml.template
cd maven-build
mvn ${mvn_opts} -DskipTests source:jar-no-fork install

if [ "$deploy" == "yes" ]; then
    mvn ${mvn_opts} -DaltDeploymentRepository=voyager-upstream::default::https://nexus.voyagerdev.com/repository/voyager-upstream -DskipTests deploy
fi

rm -rf $output_dir
mkdir -p $output_dir

set -x
mv ${mvn_repo}/org/apache/solr $output_dir
mv ${mvn_repo}/org/apache/lucene $output_dir

find $output_dir -name "maven-metadata-local.xml" -exec rm -rf {} \;
find $output_dir -name "_maven.repositories" -exec rm -rf {} \;
find $output_dir -name "_remote.repositories" -exec rm -rf {} \;

du -hs $output_dir
