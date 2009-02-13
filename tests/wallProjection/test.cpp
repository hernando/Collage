
/* Copyright (c) 2007-2008, Stefan Eilemann <eile@equalizergraphics.com> 
   All rights reserved. */

#include <test.h>

#include <eq/client/wall.h>
#include <eq/client/projection.h>

using namespace eq;
using namespace std;

// Tests the wall description
void test1();
void test2();
int main( int argc, char **argv )
{
 test1();
 //test2();
}

void test1(){
    Projection projection;
    projection.distance = 7;
    projection.fov[0] = 90;
    projection.fov[1] = 90;
    projection.hpr[0] = 65;
    projection.hpr[1] = -68;
    projection.hpr[2] = -77;
    projection.origin[0] = 0;
    projection.origin[1] = 0;
    projection.origin[2] = 0;

    
    Wall wall;
    wall = projection;

    Projection projection2;
    projection2 = wall;
    
    Wall wall2;
    wall2 = projection2;
    TESTINFO( 1 == 0, projection << wall << projection2 << wall2 );
}

void test2(){
    Wall wall;
    wall.bottomLeft  = vmml::Vector3f( 2.4749f, -9.4749f, -5.5303f );
    wall.bottomRight = vmml::Vector3f( 4.5251f, 2.4749f, -1.4497f );
    wall.topLeft     = vmml::Vector3f( -4.5251f, -2.4749, 15.45f );

    Projection projection;
       
    projection = wall;
  
    Wall wall2;
    wall2=  projection; 
    TESTINFO( wall == wall2, wall << projection << wall2 );
}