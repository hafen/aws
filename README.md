# Velocity Stack on Amazon EMR #
## Prereqs ##
*****
*   An Amazon AWS Account (EMR is not available with the free usage tier)  
    *   http://aws.amazon.com/  
*   Install the Amazon EMR CLI  
    *   http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html  
    *   Follow all the instructions!

## Instantiating a Cluster ##
*****
* Copy all emr-2.4.2/install-* scripts to your S3 Bucket (ignore the Rhipe-*tar.gz)  
* Edit the command below replacing **bucket** with your own S3 bucket and specificying the key-pair you just made  
* Run the command from the command line on your local machine where you installed elastic-mapreduce as outlined in the install guild above  
  
````
./elastic-mapreduce --create --alive --name "VelocityCluster" --enable-debugging \
--num-instances 2 --slave-instance-type m1.large --master-instance-type m3.xlarge --ami-version "2.4.2" \
--with-termination-protection \
--key-pair <Your Key Pair> \
--log-uri s3://<bucket>/logs \
--bootstrap-action "s3://<bucket>/install-preconfigure" \
--bootstrap-action "s3://<bucket>/install-r" \
--bootstrap-action s3://elasticmapreduce/bootstrap-actions/run-if --args "instance.isMaster=true,s3://<bucket>/install-rstudio" \
--bootstrap-action s3://elasticmapreduce/bootstrap-actions/run-if --args "instance.isMaster=true,s3://<bucket>/install-shiny-server" \
--bootstrap-action "s3://<bucket>/install-protobuf" \
--bootstrap-action "s3://<bucket>/install-rhipe" \
--bootstrap-action "s3://<bucket>/install-additional-pkgs"  
````

You can monitor the progress on the EMR console  
https://console.aws.amazon.com/elasticmapreduce/vnext/home
  
Once the cluster has been spun up (around 10 - 20 min) you can access the machine via ssh through the elastic-mapreduce cli  
`./elastic-mapreduce --ssh -j <job id from previous command>`  
## Notes ##
*****
*   This is based on Amazon AMI image 2.4.2.  More current AMIs come with R 3.x preinstalled and will be looked at in the future
*   Amazon Hadoop 1.0.3 comes with Google proto bufs 2.4.1.  This is based on Rhipe 0.74 which depends on proto bufs 2.4.1.  
*   Rhipe 0.75 is based on proto bufs 2.5.0 and initial testing was unsuccessful even with prot bufs 2.5 manually installed

## Known Issues ##
*****
*   "m1.large" or larger instance types must be used.  Smaller instance types have caused issues where hadoop is unable to start
*   Shiny server does not start during the bootstrapping and attempts to make it do so have not been successful.  After the cluster has started you must ssh into the master and start it manually:  
    `sudo -u shiny nohup shiny-server &`