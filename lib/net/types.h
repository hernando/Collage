
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

#ifndef EQNET_TYPES_H
#define EQNET_TYPES_H

#include <eq/base/hash.h>
#include <eq/base/refPtr.h>
#include <vector>

namespace eq
{
namespace net
{

class Node;
class Session;
class Object;
class Connection;
class ConnectionDescription;

typedef base::RefPtr< Node >                  NodePtr;
typedef base::RefPtr< Connection >            ConnectionPtr;
typedef base::RefPtr< ConnectionDescription > ConnectionDescriptionPtr;

typedef std::vector< NodePtr >                   NodeVector;
typedef stde::hash_map< uint32_t, Session* >     SessionHash;
typedef std::vector< Object* >                   ObjectVector;
typedef stde::hash_map< uint32_t, ObjectVector > ObjectVectorHash;
typedef std::vector< ConnectionPtr >             ConnectionVector;
typedef std::vector< ConnectionDescriptionPtr >  ConnectionDescriptionVector;

}
}

#endif // EQNET_TYPES_H
