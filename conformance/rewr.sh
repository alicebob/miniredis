perl -i -pe 's/.*testCommands\(t,.*/testRaw(t, func(c *client) {/' $@
perl -i -pe 's/succ\((.*)\),?/c.Do($1)/' $@
perl -i -pe 's/succSorted\((.*)\),?/c.DoSorted($1)/' $@
perl -i -pe 's/succLoosely\((.*)\),?/c.DoLoosly($1)/' $@
perl -i -pe 's/fail\((.*)\),?/c.Do($1)/' $@
perl -i -pe 's/^\s+\)\s*$/})\n/' $@
# perl -i -pe 's/\s(-?\d+e?\.?\d*)/ "$1"/g' $@

