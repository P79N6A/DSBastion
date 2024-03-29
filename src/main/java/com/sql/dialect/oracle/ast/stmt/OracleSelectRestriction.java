/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sql.dialect.oracle.ast.stmt;

import com.sql.dialect.oracle.ast.OracleSQLObjectImpl;
import com.sql.dialect.oracle.visitor.OracleASTVisitor;

public abstract class OracleSelectRestriction extends OracleSQLObjectImpl {

    public OracleSelectRestriction(){

    }

    public static class CheckOption extends OracleSelectRestriction {

        private OracleConstraint constraint;

        public CheckOption(){

        }

        public OracleConstraint getConstraint() {
            return this.constraint;
        }

        public void setConstraint(OracleConstraint constraint) {
            this.constraint = constraint;
        }

        public void accept0(OracleASTVisitor visitor) {
            if (visitor.visit(this)) {
                acceptChild(visitor, this.constraint);
            }

            visitor.endVisit(this);
        }

        public CheckOption clone() {
            CheckOption x = new CheckOption();
            if (constraint != null) {
                x.setConstraint(constraint.clone());
            }
            return x;
        }
    }

    public static class ReadOnly extends OracleSelectRestriction {

        public ReadOnly(){

        }

        public void accept0(OracleASTVisitor visitor) {
            visitor.visit(this);

            visitor.endVisit(this);
        }

        public ReadOnly clone() {
            ReadOnly x = new ReadOnly();
            return x;
        }
    }


}
