# pageup-dbt-utils
Common macros and tests for use in PageUp dbt projects

Other databases might be supported, but currently support is only guaranteed for:
 - [x] postgres


 #### Troubleshooting

**Could not find profile named 'default'**
Make sure you have a profile called `default`. You may only have one called `bakeoff`...

 **Tests: I get the error `symbolic link privilege not held` when calling `dbt deps` on windows** \
 Enable developer mode on you computer (start menu > settings > search for "use developer features") \
 If that does not work, run `dbt deps` as administrator. \
 Issue can be tracked here: https://github.com/fishtown-analytics/dbt/issues/766