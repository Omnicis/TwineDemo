#! /usr/bin/env perl 
$offset = 0;
$filler_cnt = 0;
while(<STDIN>){
    if(/^\s*@\s*(\d+)\s*(\w+)\s+\$char(\d+)\..*/){
        $pos = $1;
        $name = $2;
        $len = $3;
        
        if($offset != 0 && $offset != $pos){
            $filler_len = $pos - $offset;
            print "filler", $filler_cnt++, ": String # \$", $filler_len, "\n";
        }
        print $name, ": String # \$", $len,"\n";
        $offset = $pos + $len;
    }
}
