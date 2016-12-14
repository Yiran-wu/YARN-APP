package com.iwantfind;

import com.google.common.base.Preconditions;

import java.util.Vector;

/**
 * Created by YiRan on 12/13/16.
 */
public class CommandBuilder {
    private String mBase;
    private Vector<String> mArgs = new Vector<String>(10);


    public CommandBuilder(String base) {
        mBase = Preconditions.checkNotNull(base);
    }


    public CommandBuilder addArg(Object arg) {
        mArgs.add(String.valueOf(arg));
        return this;
    }

    public CommandBuilder addArg(String opt, Object arg) {
        mArgs.add(opt + " " + String.valueOf(arg));
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(mBase + " ");
        for (String str : mArgs) {
            builder.append(str).append(" ");
        }
        return builder.toString();
    }
}
