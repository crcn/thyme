
//parses 

module.exports = function(triggerDate)
{                                   
	//specific date
	if(triggerDate instanceof Date)
	{
		return triggerDate;
	}   
	
	var currentTime = new Date();
	 
	//timeout in MS
	if(triggerDate instanceof Number)
	{        
		currentTime.setTime(currentTime.getTime() + triggerDate);
		            
		return currentTime;
	}
	                                                 
	
	function timesToNumber(stack)
	{
		for(var i = stack.length; i--;)
		{                                    
			stack[i] = Number(stack[i]);
		}            
		
		return stack;
	}
	     
	var codes = triggerDate.toString().split(' '),
		ret = {};
	                           
	//time-1:30 dow-1,2,3,4,5,6 
	for(var i = codes.length; i--;)
	{
		var code = codes[i].split('-'),

		type = code[0].toUpperCase(),
		times = code[1].toUpperCase().split(',');    

		if(type == 'DOW' || type == 'W')
		{     
			ret.daysOfWeek = timesToNumber(times);     
		}
		else
		if(type == 'DOM' || type == 'M')
		{                               
			ret.daysOfMonth = timesToNumber(times);
		}
		else 
		if(type == 'TIMES' || type == 'T')
		{                                    
			for(var j = times.length; j--;)
			{                           
				var time = times[j].split(':'),
				hour = Number(time[0]),
				minute = Number(time[1]); 

				times[j] = time = { hour: hour, minute: minute || 0 };    
			}                                           
			
			ret.timesOfDay = times;        
		}
                            
	}   
	
	                   
	return setNextDate(ret);   
}   
	
function getTimeInc(currentTime, max, checkOrTime)
{         
	var timeInc = 0, check;
	                                         
	if(typeof checkOrTime != 'function')
	{                             
		
		var times = checkOrTime instanceof Array ? checkOrTime : [checkOrTime];
		                       
		check = function(time)
		{
			return times.indexOf(time)+1;
		}        
	}                                       
	else
	{
		check = checkOrTime;
	}                                                                                               
	
	if(!check(currentTime))
	{                   
		//first check this week
		for(var newTime = currentTime; newTime <= max; newTime++)
		{
			if(check(currentTime)) break;  
			timeInc++;
		}           
		                      
		
		//then next week
		if(!check(newTime))
		{
			for(var newTime = 0; newTime < currentTime; newTime++)
			{
				if(check(newTime)) break;  
				timeInc++;  
			}        
		}
	}   

	return timeInc;
}    
	
function setNextDate(times)
{
	var currentTime = new Date(),
	chour = currentTime.getHours(),
	cminute = currentTime.getMinutes(),
	cdow = currentTime.getDay(),
	cdom = currentTime.getDate(),
	cmonth = currentTime.getMonth();
	
	var ntime,
		ndom = cdom,
		nmonth = cmonth,
		nyear  = currentTime.getFullYear();
                                                         
	         
	if(times.timesOfDay)
	{                 
		                                
		for(var i = 0, n = times.timesOfDay.length; i < n; i++)
		{
			var tod = times.timesOfDay[i];    
			
			ntime = tod; 
			
			if(chour == tod.hour && cminute < tod.minute)
			{
				break;
			}         
			else
			if(chour < tod.hour)   
			{
				break;
			}
		}                 
		                      
		if(i == n)
		{
			ndom++;
		}
	}
	
	
	if(times.daysOfWeek)
	{                    
		ndom += getTimeInc(cdow, 6, times.daysOfWeek);        
	}
	else
	if(times.daysOfMonth)
	{           
		ndom += getTimeInc(cdom, 31, times.daysOfMonth);        
	} 
	
	if(ndom > 31)
	{     
		//subtract the difference    
		ndom = ndom-31;
		                                       
		if(cmonth < 11)
		{
			nmonth++;
		}            
		else
		{     
			nmonth = 0;      
			nyear++;
		}                                       
	} 
	
	//if the day has changed, and there ARE times, then set
	if(ndom > cdom && times.timesOfDay)
	{
		ntime = times.timesOfDay[0];
	}   
	
	var nextTime = new Date(0);
	
	if(ntime)
	{                            
		nextTime.setHours(ntime.hour);
		nextTime.setMinutes(ntime.minute);     
	}                                                  
	
	nextTime.setDate(ndom);
	nextTime.setMonth(nmonth);
	nextTime.setFullYear(nyear);
	                
	times.nextCall = nextTime;   
	                       
	return times;
}                
	    