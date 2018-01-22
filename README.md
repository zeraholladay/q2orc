# q2orc

# Windows setup

$ curl https://codeload.github.com/steveloughran/winutils/zip/master > winutils-master.zip
$ unzip winutils-master.zip
$ export HADOOP_HOME=$PWD/winutils-master/hadoop-X.Y.Z
$ export PATH=$HADOOP_HOME/bin:$PATH

mvn clean compile assembly:single

