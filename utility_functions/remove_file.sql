/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_remove_file( filepath text) 
RETURNS boolean AS

    $BODY$ 

    import os   

    if os.path.exists(filepath):
        os.remove(filepath)
        return True
    else:
        plpy.ERROR("File: %s does not exist" % filepath)
        return False

    $BODY$

LANGUAGE plpythonu VOLATILE;