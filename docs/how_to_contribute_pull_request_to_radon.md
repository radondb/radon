Contents
=================

* [how_to_contribute_pull_request_to_radon](#how_to_contribute_pull_request_to_radon)
   * [Requirements](#requirements)
   * [Step1. Fork a radon repo to your git](#step1-fork-a-radon-repo-to-your-git)
   * [Step2. Clone radon repo to your local machine](#step2-clone-radon-repo-to-your-local-machine)
   * [Step3. Modify sth. on local branch and make a commit](#step3-modify-sth-on-local-branch-and-make-a-commit)
   * [Step4. Push the local modification to your github](#step4-push-the-local-modification-to-your-github)
   * [Step5. Pull a request to base repo](#step5-pull-a-request-to-base-repo)

# how_to_contribute_pull_request_to_radon

## Requirements

1. Make sure that git has being installed on your system and it will be helpful if you are already familiar with the use of git.
2. The following operations are tested on linux/OS X terminal,  you may need a  git bash for windows.
3. A `GitHub account` is required, if you don't have git account, register one on [GitHub](https://github.com).

## Step1. Fork a radon repo to your git

Signed in GitHub, and open the [radon address on github](https://github.com/radondb/radon), on the top right corner, you will see a `Fork` tag, click on and fork radon to your git account.

## Step2. Clone radon repo to your local machine

Create a local directory and execute command like : `git clone your_forked_radon_addr_on_github`, it will generate a local radon repo. 
For example, I fork a radon repo to my GitHub from base radon repo and the git address after fork is [https://github.com/hustjieke/radon](https://github.com/hustjieke/radon), the clone operation will be next(`note: use your own git address when execute git clone command`):

```
$ mkdir my_radon
$ cd my_radon
$ git  clone https://github.com/hustjieke/radon
Cloning into 'radon'...
remote: Counting objects: 825, done.
remote: Total 825 (delta 0), reused 0 (delta 0), pack-reused 825
Receiving objects: 100% (825/825), 806.93 KiB | 354.00 KiB/s, done.
Resolving deltas: 100% (149/149), done.
```
```
$ ls
radon
$ cd radon
$ ls 
LICENSE   README.md conf      docs      makefile  src
```

## Step3. Modify sth. on local branch and make a commit
`e.g.` If you make a modification on some file like README.md, execute the following commands to save the changes. `note`: you may need push permissions when you first execute `git push`.
```
$ git add README.md 
$ git commit -m "README.md: some words spell error"
[master 771154a] README.md: some words spell error
 1 file changed, 1 insertion(+), 1 deletion(-)
```

## Step4. Push the local modification to your github

Before you commit local modifications to your radon branch on GitHub, make sure that test cases are all passed. If some error happens, the result will be failed, you should correct them before you push the modifications to GitHub. 
```
$ ls
LICENSE   README.md conf      docs      makefile  src
$ make test
--> Testing...
go test -v -race xbase
=== RUN   TestXbaseWriteFile
--- PASS: TestXbaseWriteFile (0.00s)
=== RUN   TestXbaseTruncateQuery
--- PASS: TestXbaseTruncateQuery (0.00s)
=== RUN   TestDiskUsage
--- PASS: TestDiskUsage (0.00s)
=== RUN   TestHttpGet
--- PASS: TestHttpGet (0.11s)
=== RUN   TestHttpPost
--- PASS: TestHttpPost (0.11s)
.....
.....
.....
--- PASS: TestCtlV1DropUser (0.53s)
=== RUN   TestCtlV1DropError
--- PASS: TestCtlV1DropError (0.54s)
PASS
ok  	ctl/v1	38.603s
```

After all test cases are all passed, execute git command `git push`. 

```
$ git push
Counting objects: 3, done.
Delta compression using up to 8 threads.
Compressing objects: 100% (3/3), done.
Writing objects: 100% (3/3), 278 bytes | 278.00 KiB/s, done.
Total 3 (delta 2), reused 0 (delta 0)
remote: Resolving deltas: 100% (2/2), completed with 2 local objects.
To https://github.com/hustjieke/radon.git
   43d8671..8f1585d  feature_test -> feature_test
$
```
Now your radon repo on GitHub is even with local radon repo.

## Step5. Pull a request to base repo

Before you submit a pull request to base repo,  execute `fetch` command so that the latest updates on base repo are merge to you local repo. You should add an upstream pointed to base repo before you make a fetch.

```
$ git remote -v
origin	https://github.com/hustjieke/radon.git (fetch)
origin	https://github.com/hustjieke/radon.git (push)
$ git remote add upstream https://github.com/radondb/radon.git
$ git remote -v
origin	https://github.com/hustjieke/radon.git (fetch)
origin	https://github.com/hustjieke/radon.git (push)
upstream	https://github.com/radondb/radon.git (fetch)
upstream	https://github.com/radondb/radon.git (push)
$ git fetch upstream
remote: Counting objects: 59, done.
remote: Total 59 (delta 33), reused 33 (delta 33), pack-reused 26
Unpacking objects: 100% (59/59), done.
From https://github.com/radondb/radon
 * [new branch]      master     -> upstream/master
```


Then merge upstream branch to master:
```
$ git merge upstream/master
Merge made by the 'recursive' strategy.
 .gitignore                         |  3 +++
 README.md                          |  8 ++++----
 conf/radon.default.json            |  2 +-
 docs/how_to_build_and_run_radon.md | 30 ++++++++++++++++--------------
 src/binlog/binlog.go               |  2 +-
 src/binlog/event.go                |  2 +-
 src/binlog/info.go                 |  2 +-
 src/binlog/io.go                   |  2 +-
 src/binlog/mock.go                 |  2 +-
 src/binlog/sql.go                  |  2 +-
 src/syncer/meta.go                 |  2 +-
 src/syncer/meta_test.go            |  2 +-
 src/syncer/peer.go                 |  2 +-
 src/syncer/peer_test.go            |  2 +-
 src/syncer/syncer.go               |  2 +-
 src/syncer/syncer_test.go          |  2 +-
 16 files changed, 36 insertions(+), 31 deletions(-)
 $
```

Push the local updates to GitHub.

```
$ git push
 1 file changed, 1 insertion(+), 1 deletion(-)
```

Finally, you can submit a pull request from your radon repo on GitHub to base radon repo. Find the `New pull request` button on your repo and click on it, it will change to a new html and a dialogue box  appears. You can write some comments and then click on  `Create pull request` button. Now the bot will begin to execute `make test` and `make coverage`, if test cases are all passed, the pull request is success  and submitted to base repo, if the owner accept your request, congratulation!
