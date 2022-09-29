# pschema

Pregel based schema validation algorithm

# More information

Preprint paper that describes the main motivation for the library and its use case to create subsets of Wikidata (section 5.4): [Creating Knowledge Graphs Subsets using Shape Expressions](https://arxiv.org/abs/2110.11709), [Jose Emilio Labra Gayo](http://labra.weso.es)

# Contributors

- [Jose Emilio Labra Gayo](http://labra.weso.es)

# Software that depends on this library

- [sparkwdsub](https://github.com/weso/sparkwdsub)

## Publishing to OSS-Sonatype

This project uses [the sbt ci release](https://github.com/olafurpg/sbt-ci-release) plugin for publishing to [OSS Sonatype](https://oss.sonatype.org/).

##### SNAPSHOT Releases
Open a PR and merge it to watch the CI release a -SNAPSHOT version

##### Full Library Releases
1. Push a tag and watch the CI do a regular release
2. `git tag -a v0.1.0 -m "v0.1.0"`
3. `git push origin v0.1.0`
_Note that the tag version MUST start with v._
