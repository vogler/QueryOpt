close all; clear all; clc;

m = 100;
n = 10;

N = n*m;
Y = zeros(1,N);     % Yao
Y_w = zeros(1,N);   % Waters
Y_b = zeros(1,N);   % Bernstein


for k = 1:1000
    if (k<=N-n) 
        % Yao
        %p = nchoosek(N-n,k)/nchoosek(N,k); %not exact
        X = 0:k-1;
        p = prod((N-n-X)./(N-X));
        Y(k)   = m*(1-p);
        % Waters
        Y_w(k) = m*(1-(1-k/N)^n);
    else
        Y(k) = m;
        Y_w(k) = m;
    end
    
    % Bernstein
    if (k<m/2)
        Y_b(k) = k;
    elseif (k<2*m)
        Y_b(k) = (k+m)/3;
    else
        Y_b(k) = m;
    end
end

f1 = figure;
hold all;
ylabel('accessed pages');
xlabel('tuples');
f2 = copyobj(gcf,0);
f3 = copyobj(gcf,0);
f4 = copyobj(gcf,0);
figure(f1);
title('Combined plot');
plot(Y, 'Color', 'Red');
plot(Y_w, 'Color', 'Blue');
plot(Y_b, 'Color', 'Green');
legend('Yao', 'Waters', 'Bernstein');

figure(f2);
title('Yao');
plot(Y, 'Color', 'Red');

figure(f3);
title('Waters');
plot(Y_w, 'Color', 'Blue');

figure(f4);
title('Bernstein');
plot(Y_b, 'Color', 'Green');
