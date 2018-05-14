master: ![travis build status](https://travis-ci.org/PageUpPeopleOrg/pageup-dbt-utils.svg?branch=master)

# pageup-dbt-utils
Common macros and tests for use in PageUp dbt projects

Other databases might be supported, but currently support is only guaranteed for:
 - [x] postgres


## Troubleshooting

### Could not find profile named 'default'

Make sure you have a profile called `default`. You may only have one called `bakeoff`...

### Tests: I get the error `symbolic link privilege not held` when calling `dbt deps` on windows

* Run `dbt deps` as administrator, this is a known issue with dbt.
 Issue can be tracked here:  https://github.com/fishtown-analytics/dbt/issues/766
* Alternatively, run `run-tests.ps1` which will ask for admin access when needed.
* If issue has been fixed, you may need to still enable developer mode on you computer (start menu > settings > search for "use developer features")
