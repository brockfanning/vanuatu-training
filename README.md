# Open SDG - Simple starter

This is a starter repository to help in implementing the [Open SDG](https://open-sdg.org) platform. [See here for documentation](https://open-sdg.readthedocs.io) on Open SDG.

## Overview

This starter repo is called "simple" because it combines the data and site into one repository. This is in contrast to the [open-sdg-site-starter](https://github.com/open-sdg/open-sdg-site-starter) and [open-sdg-data-starter](https://github.com/open-sdg/open-sdg-data-starter) repos, which together make an alternative approach that splits the site and data into separate repositories.

## Requirements

* Python 3 with [pip](https://pypi.org/project/pip/)
* Ruby 2 with [bundler](https://bundler.io/)

## Getting started

1. Make your own copy of this repository, by pressing "Use this template" or going [here](https://github.com/open-sdg/open-sdg-simple-starter/generate) and filling out the form.

   > For "Repository name" you can enter "open-sdg-simple-starter" or whatever you would like. Note that if you do *not* use "open-sdg-simple-starter", you will need to perform an extra step at the end (see step 6).

    When ready, press "Create repository from template".

2. You will now be looking at your new repository. Some automated processes will already be started. To see them, click the "Actions" link. Wait until the "Initial commit" action stops spinning and changes from yellow to green.
3. Click the "Settings" link and scroll down to the "Github Pages" section. In the dropdown beneath "Source" choose "gh-pages" and press "Save".
4. Github will start to deploy your site behind the scenes. Wait about 1 minute, and then refresh the page.
5. Again scroll down to the "Github Pages" section. You should now see a green success notice that says "Your site is published". Press the link to view your site.
6. If (in step 1) you named your repository *anything other than* "open-sdg-simple-starter", your site will not look or behave correctly yet. To fix this, you will need to update your site's "baseurl" setting.
    1. Go back to your repository on Github and click the "Code" link (top left).
    2. In the list of files find the file called "config_site.yml" and click it.
    3. Click the pencil icon towards the top right to edit this file.
    4. On line #9, update the "baseurl" setting with your repository's name, preceded by a slash. For example, if you named your repository "my-repository", then set the "baseurl" to "/my-repository".
    5. At the bottom press "Commit changes".
    6. Watch the "Actions" section until the action stops spinning and changes from yellow to green. At this point, your site should look and behave correctly.

## Automated testing

Each time you update your site there will be 3 steps:

1. Edit one or more files
2. Create a pull request
3. Merge that pull request

This repository is capable of automatically testing all pull requests, to ensure that nothing will be broken. You can enforce this by requiring that all tests pass before a pull request is merged.

To enable this, start by making any change using the 3-step workflow above. Try a simple README change:

1. In the list of files, click on README.md.
2. Click the pencil icon on the right (You can find it next to the History button.)
3. Make some changes to the file. (Any change is fine.)
4. Towards the bottom, select "Create a new branch for this commit and start a pull request."
5. Beneath this, click "Propose file changes".
6. Click on the green "Create pull request" button.
7. Wait a moment to see the message that says "Test PRs / test (pull_request) - in progress"
8. Wait until you see "All checks have passed". This takes about 5 minutes.
9. Click on the green "Merge pull request" button.

Next, go back to your "Settings" page.

1. In the left sidebar, click "Branches".
2. Under "Branch protection rules" click "Add rule"
3. Under "Branch name pattern" enter develop
4. Check the box "Require status checks to pass before merging"
5. Under "Status checks found in the last week for this repository" check the box for "test".
6. Click the green "Create" button.

## Local installation

To try out Open SDG using this starter on your local computer, use:

```
make install
make serve
```
