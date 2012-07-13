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


plot(Y, 'Color', 'Red');
hold all;
plot(Y_w, 'Color', 'Blue');
hold all;
plot(Y_b, 'Color', 'Green');
legend('Yao', 'Waters', 'Bernstein');
title('Combined plot');
ylabel('accessed pages');
xlabel('tuples');

figure;
plot(Y, 'Color', 'Red');
title('Yao');
ylabel('accessed pages');
xlabel('tuples');

figure;
plot(Y_w, 'Color', 'Blue');
title('Waters');
ylabel('accessed pages');
xlabel('tuples');

figure;
plot(Y_b, 'Color', 'Green');
title('Bernstein');
ylabel('accessed pages');
xlabel('tuples');
