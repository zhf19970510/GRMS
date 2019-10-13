// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   IdTagGroupComparator.java

package com.briup.grms.step4;

import org.apache.hadoop.io.*;

// Referenced classes of package com.briup.grms.step4:
//			IdTag

public class IdTagGroupComparator extends WritableComparator
{

	public IdTagGroupComparator()
	{
		super(com/briup/grms/step4/IdTag, true);
	}

	public int compare(WritableComparable a, WritableComparable b)
	{
		IdTag it1 = (IdTag)a;
		IdTag it2 = (IdTag)b;
		return it1.getId().compareTo(it2.getId());
	}
}
