# How to contribute
We welcome contributions from the community and are pleased to have them.
Please follow this guide when logging issues or making code changes. If unsure
about anything, you can [open an issue with your query.][newIssue]

## Logging Issues
All issues should be created using the [new issue form][newIssue].  Clearly
describe the issue including steps to reproduce if there are any.  Also, make
sure to indicate the earliest version that has the issue being reported.

## Patching Code
Code changes are welcome and should follow the guidelines below.

* Fork the repository on GitHub.
* Fix the issue ensuring that your code passing the linting test.
* Add tests for your new code ensuring that it is fully tested.
* Ensure all tests pass by running `grunt build`
* [Pull requests][pr] should be made to the [master branch][master].

[Adapted with permission from senecajs.](https://github.com/senecajs/seneca/)

[newIssue]: https://github.com/apparatus/kafkaesque/issues/new
[pr]: http://help.github.com/send-pull-requests/
[master]: https://github.com/apparatus/kafkaesque/tree/master
