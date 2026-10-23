package dev.aleksandr.metainfo.jolt.transform;

import static dev.aleksandr.metainfo.jolt.transform.SetDateNowDateToListTransform.nowDateString;

public class SetDateNowDateToVariableTransform extends ContextualTransformExt {

    public static final String VAR_NAME_KEY = "variableName";

    @Override
    public Object transform(Object input, OperationContext ctx) {
        String varName = ctx.get(VAR_NAME_KEY);
        ctx.putGlobalVar(varName, nowDateString());
        return input;
    }
}
