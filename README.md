# arc2arc_primer
Scripts demonstrating what ANS changes to make when transforming ANS of various objects for ingestion to a new target Arc org, or from an org's production to sandbox environments
**This is not production code.**

Setup
==================
Create a virtual environment, activate it and install the script requirements.

``$ pip3 install -r requirements.txt``

You will need access to one Arc organization id and bearer token to run scripts that copy objects from an Arc organization's production environment to sandbox environment.

You will need access to two Arc organization ids and bearer tokens for each to run scripts that copy objects from one Arc organization to another Arc organization.

## Pycharm Configuration and Debugging

Using the PyCharm IDE you can run this code locally and will have the ability to inspect its methods and processes.

Once the repo is installed locally and the virtual environment is created -- lets assume the virtual env is named `arc2arc-transformations` -- open the location of the repo in Pycharm as a project.

``Pycharm menu > File > Open > {navigate to repository root} > click Open button > {choose to open repository project in new window}``

Create a PyCharm configuration and point it to the virtual environment that has already been set up.

```Pycharm menu > PyCharm > Preferences > Project: inbound-feeds-poc > Python Interpreter```

If the virtual environment is not already listed in the drop down: 

- select the gear icon
- select "Add..."
- in the window that opens, select "Existing Environment"
- select "..." icon at the end of the interpreter drop down
- in the window that opens, navigate to the location of the virtual environment folder
- in the virtual environment folder look for the python bin file, likely in `/environment/bin` folder
- save your changes to complete

PyCharm will take some time to rebuild its indexes.  With the Interpreter set up you can create a debugging configuration.  Once set up, you can run this configuration which will start up a locally running Flask server.  From this server you can run the adapter locally and if you set debugging breakpoints, interrupt the flow of control and inspect the application in action.

With the repository open in PyCharm, locate the drop down across the top of the window. This drop down may appear empty, but if you select it you will se there is an option within, "Edit Configurations".

- select "Edit Configurations" from the drop down, opening the Edit Configurations window
- select the plus (+) icon
- select Python from menu
- set Script Path to `/arc2arc_primer/{name-of-script}.py`
- set Working Directory to `/arc2arc_primer/`
- verify that the Python Interpreter has been automatically set to the correct value
- save your changes to complete

Now either the green arrow icon or the green bug icon next to the configurations drop down will launch and run the POC.

## Run Script from terminal

You may run the application directly from the terminal.

``$ PYTHONPATH=.  python arc2arc_primer/{name-of-script}.py ``

Each script works by passing in a series of script parameters. Each script will require some of the following parameters:
- the source organization id
- the target organization id
- the id of the content
- an Arc bearer token of the source organization
- an Arc bearer token of the target organization
- the website id of the target organization for the location of the transformed content
- the site section id in the website of the target organization for the location of the transformed content
- a boolean to direct the script to do a dry run and not save the transformed ANS to Arc
- a number of test iterations to run before exiting out of a script that operates on more than one piece of content

For example:
``$ PYTHONPATH=.  python arc2arc_primer/01_transform_story.py --from-org devtraining --to-org cetest --story-arcid MBDJUMH35VA4VKRW2Y6S2IR44A --from-token <token> --to-token <token> --to-website-site cetest --to-website-section /test  --dry-run 1``

-----------
Scripts
==================
These scripts are written to show how to tranform an object of a particular Arc type such that it can be ingested from a source Arc organization to a different Arc organization, or from the production environment of an Arc organization to the sandbox organization of the same organization.

The arc2arc_primer is collection of Python scripts where each script is showing you the very basics of what you need to change in an object’s ANS so that it can be ingested from one source Arc Organization to another Arc Organization. You can think of each script as a story written in proccessable code, rather than as a robust application.

These are scripts that you will be able to run on a command line, on individual objects in your own Arc organizations, and view the resulting transformations either as a dry-run, where no object is created but where you will view the changed ANS, or as a process that will create a new object and also show you some information about what changes may still need to come.

Most scripts transform a single item of content. These scripts are not a content adapter or an application. These scripts are a demonstration to show the transformation steps necessary in an Arc to Arc migration, and can be used as the stepping off point for putting together an Arc to Arc adapter.

### 01_transform_story.py
Transform one story ANS using its Arc ID, from one Arc organization to a second Arc organization, in the production environment.

### 02_transform_story_to_sandbox.py
Transform one story ANS using its Arc ID, from an Arc organization's production environment to its sandbox environment.

### 03_transform_video.py
Transform one video ANS using its Arc ID, from one Arc organization to a second Arc organization, in the production environment.

### 04_transform_video_to_sandbox.py
Transform one video ANS using its Arc ID, from an Arc organization's production environment to its sandbox environment.

### 05_transform_gallery.py
Transform one gallery ANS using its Arc ID, from one Arc organization to a second Arc organization, in the production environment.
During this process the gallery Arc ID will change to a new value. It is not possible to have Photo Center Arc IDs that are the same between different organizations.
This restriction only applies to Photo Center objects.

### 06_transform_gallery_to_sandbox.py
Transform one gallery ANS using its Arc ID, from an Arc organization's production environment to its sandbox environment.
During this process the gallery Arc ID will change to a new value. It is not possible to have Photo Center Arc IDs that are the same between different organizations.
This restriction only applies to Photo Center objects.

### 07_transform_image.py
Transform one image ANS using its Arc ID, from one Arc organization to a second Arc organization, in the production environment.
During this process the gallery Arc ID will change to a new value. It is not possible to have Photo Center Arc IDs that are the same between different organizations.
This restriction only applies to Photo Center objects.

### 08_transform_image_to_sandbox.py
Transform one image ANS using its Arc ID, from an Arc organization's production environment to its sandbox environment.
During this process the gallery Arc ID will change to a new value. It is not possible to have Photo Center Arc IDs that are the same between different organizations.
This restriction only applies to Photo Center objects.

### 09_transform_author.py
Transform one author object using its author id, from one Arc organization to a second Arc organization, in the production environment.

### 10_transform_authors_all.py
Transform all author objects from one Arc organization to a second Arc organization, in the production environment.

### 11_transform_redirects_all.py
Transform all document redirects from one Arc organization to a second Arc organization, in the production environment.  
Will work well for story document redirects, is potentially problematic for video or gallery document redirects.
 
### 12_transform_lightbox.py
Transform one lightbox via its lightbox id from one Arc organization's production environment to a target organization's production environment.

### 13_transform_collection.py
Transform one collection via its ans id from one Arc organization's production environment to a target organization's production environment.

### arc_endpoints.py
Methods wrapping the Arc APIs so they can be more easily used from within the transformation scripts.

### arcid.py
A method that creates a new arc id, used from within the transformation scripts.

### dist_ref_id.py
Methods that work to create new distributors and geographic references, used from with the transformation scripts.