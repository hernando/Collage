
/* Copyright (c) 2006-2009, Stefan Eilemann <eile@equalizergraphics.com>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *  
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <test.h>
#include <eq/base/referenced.h>
#include <eq/base/refPtr.h>
#include <eq/base/thread.h>
#include <iostream>

using namespace eq::base;
using namespace std;

#define NTHREADS 16
#define NREFS    300000

class Foo : public Referenced
{
public:
    Foo() {}
private:
    virtual ~Foo() {}
};

typedef RefPtr<Foo> FooPtr;
FooPtr foo;

class TestThread : public Thread
{
public:
    virtual void* run()
        {
            FooPtr myFoo;
            for( size_t i = 0; i<NREFS; ++i )
            {
                myFoo = foo;
                foo   = myFoo;
                myFoo = 0;
            }

            return EXIT_SUCCESS;
        }
};

int main( int argc, char **argv )
{
    TestThread threads[NTHREADS];

    Clock clock;
    foo = new Foo;
    for( size_t i=0; i<NTHREADS; ++i )
        TEST( threads[i].start( ));

    for( size_t i=0; i<NTHREADS; ++i )
        TEST( threads[i].join( ));

    const float time = clock.getTimef();
    cout << time << " ms for " << 3*NREFS << " reference operations in " 
         << NTHREADS << " threads (" << time/(3*NREFS*NTHREADS)*1000000
         << "ns/op)" 
         << endl;

    TEST( foo->getRefCount() == 1 );
    foo = 0;

    return EXIT_SUCCESS;
}

