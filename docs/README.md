# Github Workflow
## Begin Work and Start New Branch (In Windows Powershell)
cd <directory path>     # Navigate to your cloned repo
code .                  # Open the folder in VS Code
git checkout -b <new-branch-name>  # Create and switch to a new branch

## Make your Changes in the Branch in VSCode
git add .
git commit -m "<brief description of changes>"
git push --set-upstream origin <new-branch-name>

## Create Pull Request
--Go to the repo pull request tab--
--click "New Pull Request, set base = main, compare= <new branch name>--
--click "Create Pull Request" --
--Review the PR--
--click "merge pull request--
--click "delete branch"--

## Close Branch
git checkout main
git pull
git branch -d <new-branch-name>      # Use -D if needed
git remote prune origin              # Clean up deleted remote tracking refs


