package com.iwantfind;

import com.google.common.base.Preconditions;

import java.util.Vector;

/**
 * Created by YiRan on 12/13/16.
 */
public class CommandBuilder {
    private String mBase;
    private Vector<String> mArgs = new Vector<String>(10);

    /**
     * @param base the base string to use for constructing the command, e.g. for echo "hello world"
     *        this would be "echo"
     */
    public CommandBuilder(String base) {
        mBase = Preconditions.checkNotNull(base);
    }

    /**
     * Adds the string value of the given argument to the command.
     *
     * @param arg the argument to add
     * @return the {@link CommandBuilder} with the argument added
     */
    public CommandBuilder addArg(Object arg) {
        mArgs.add(String.valueOf(arg));
        return this;
    }

    /**
     * Adds the string value of the given option argument to the command.
     *
     * For example, to add "-name myFile" to construct the command "find . -name myFile", opt should
     * be "-name" and arg should be "myFile".
     *
     * @param opt the option flag
     * @param arg the argument
     * @return the {@link CommandBuilder} with the argument added
     */
    public CommandBuilder addArg(String opt, Object arg) {
        mArgs.add(opt + " " + String.valueOf(arg));
        return this;
    }

    // Get final command
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(mBase + " ");
        for (String str : mArgs) {
            builder.append(str).append(" ");
        }
        return builder.toString();
    }
}
