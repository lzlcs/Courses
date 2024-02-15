# Google Cloud Platform Setup Instructions #

For performance testing, you will need to run it on a VM instance on the Google Cloud Platform (GCP). We've already sent you student coupons that you can use for billing purposes. Here are the steps for how to get setup for running on GCP.

NOTE: For those working in teams, it might be desirable for both students to use the same virtual machine. To do so, only one of you should first create the VM instance following the instructions below. Then, follow the instructions at https://cloud.google.com/compute/docs/access/granting-access-to-resources to grant access to your partner.

NOTE #2: __Please don't forget to SHUT DOWN your instances when you're done with your work for the day!  The 32 vCPU-enabled cloud VM instances you create for this assignment cost approximately $1 per hour.  Leaving it on accidentally for a day could quickly eat up your $50 per student quota for the assignment.__

NOTE #3: __Update your CPU quota (across all regions) under IAM & admin to at least 32 ASAP.
   
### Creating a 32 vCPU VM on GCP ###
      
1. Now you're ready to create a VM instance. Click on the button that says `Create Instance`. Fill out the form such that your cloud-based  VM has the following properties: 
  * Region __us-west1__ (Oregon)
  * Type n1-standard-32 (__32 vCPUs__, __120 GB__ memory) 
  * Ubuntu __18.04 LTS__  
  * At least a __20GB__ Standard persistent disk.
  
Notice the __>$700 monthly cost if you don't shutdown your instance.__

2. Click `Create` to create a VM instance with the above parameters. Now that you've created your VM, you should be able to __SSH__ into it. Either open it from the VM instance page (the one that shows a list of all your VM instances) or use the `gcloud compute ssh` command from your local console or the Google Cloud shell). For example:
~~~~
gcloud compute ssh MY_INSTANCE_NAME
~~~~

You can find `MY_INSTANCE_NAME` in the *VM instances* page.

3. Once you SSH into your VM instance, you'll want to install whatever software you need to make the machine a useful development environment for you.  For example we recommend:
~~~~
sudo apt update
sudo apt install emacs25
sudo apt install make
sudo apt install g++
~~~~

If you're confused about any of the steps, having problems with setting up your account or have any additional questions, reach us out on Piazza!
  
__Again, please don't forget to SHUT DOWN your instances when you're done with your work for the day!__
