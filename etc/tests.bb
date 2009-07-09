--[ TITo ]
    { a=>1, b=>2 }

--[ rowh_RANDy ]    
    {name => join "", rand_chars( set=> "alpha", min=>5, max=>7) } 

--[ rowh_RAND2y ]
     {name => join "", rand_chars( set=> "numeric", min=>5, max=>7) }

--[ h_RAND3 ]
    [ { id => (join '',rand_chars(set=>"numeric")), name => join ('', rand_chars(set=>"alpha")), role_code => 1 }, ],
