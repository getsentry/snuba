1. Please pull all the past issues over the last 24 hours from sentry for the snuba project
2. Spend time to understand every single issue and write a brief 2-4 sentence summary of them in claudeinvestigation/allissues.md
3. Now determine which of these is the most urgent, and do an in depth root cause analysis of the it, using the snuba codebase for help.
  Once you have determined the root cause please detail your findings in a file topissue.md including the following:
  * issue name
  * overview of the issue and its root cause
  * detailed root cause explaination
4. create a detailed implementation plan to fix the bug and write the plan to bugfix.md, detailed enough to resume from if you had no context.
5. please clear your context and then implement the bugfix described in bugfix.md. Please follow test driven development red-green standards for
implementing your bug fix. Validate using your tests that your implementation is correct, do not stop until your test are passing (or you give up)
6. Once you have your tests passing and the bug fix implemented please create a PR for it and push it to github with a detailed description
(I suspect you may not have access to github right now so if this is the case just create a local git branch and git commit with the pr description in markdown file)

Please take as long as you need for this task and spawn as many subagents as you like, it will be running overnight.
Do not prompt for any user input as I will be away from the computer and unable to response, operate fully autonomous from beginning to completion.
